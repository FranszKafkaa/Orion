# Orion

Ferramenta de teste de carga e benchmark HTTP escrita em Go puro, seguindo o **Open Model** de injeção de usuários do [Gatling](https://gatling.io/docs/gatling/reference/current/core/injection/#open-model).

---

## O que é o Open Model?

A maioria das ferramentas de teste de carga usa o **Closed Model**: um pool fixo de workers que só envia uma nova requisição após receber a resposta da anterior. Isso significa que, quando o servidor desacelera, a taxa de requisições por segundo também cai — e você acaba medindo *throughput* em vez de *pressão real*.

O **Open Model** funciona diferente: um `time.Ticker` dispara em intervalos fixos e **sempre** injeta um novo usuário virtual (goroutine), independentemente de quantas requisições ainda estão em voo. Se o servidor demorar 2 segundos para responder, as goroutines acumulam — mas a taxa de injeção é mantida. É assim que usuários reais se comportam.

```
Closed Model                    Open Model (carga)
────────────────                ──────────────────────────────────────
VU1: ▶──────◀▶──────◀          Tick 1ms:  ▶ lança VU1
VU2:   ▶──────◀▶───◀           Tick 2ms:  ▶ lança VU2  (VU1 ainda em voo)
VU3:     ▶────◀▶────◀          Tick 3ms:  ▶ lança VU3  (VU1 e VU2 em voo)
         ↑ taxa cai             ↑ taxa constante, pressão real
         se servidor lento
```

---

## Arquitetura interna

```
┌──────────────────────────────────────────────────────────────────┐
│                          main goroutine                          │
│  time.Ticker (1/RPS) ──► spawn VU goroutine  ──► wg.Add(1)      │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ N goroutines simultâneas
                    ┌────────────▼─────────────┐
                    │      VU goroutine         │
                    │  POST /endpoint           │
                    │  context.WithTimeout(5s)  │
                    │  chan <- result{latency,   │
                    │          status, err}      │
                    └────────────┬──────────────┘
                                 │ canal bufferizado
                    ┌────────────▼──────────────┐
                    │    collector goroutine     │
                    │  única escritora de        │
                    │  métricas (sem mutex)      │
                    │  HDR Histogram (µs)        │
                    └────────────┬──────────────┘
                                 │ após wg.Wait() + close(ch)
                    ┌────────────▼──────────────┐
                    │         report()           │
                    │  min / p50 / p95 / p99 /   │
                    │  p99.9 / max / mean        │
                    └───────────────────────────┘
```

**Propriedades de concorrência:**
- Sem `sync.Mutex` no caminho quente: VUs apenas fazem `chan <- result`, operação não-bloqueante.
- O `collector` é a única goroutine que lê o canal e escreve nas métricas — sem race conditions.
- `vuSeq` (contador de `user_id`) é incrementado via `atomic.AddInt64` — seguro para N goroutines simultâneas.
- O canal é bufferizado para `RPS × (duração + 5s) + 10.000` entradas — VUs nunca bloqueiam no envio.

---

## Instalação

**Pré-requisito:** Go 1.22+

```bash
git clone <repo>
cd carga
go mod tidy
go build -o carga .
```

Ou execute diretamente sem compilar:

```bash
go run . -url http://localhost:8080/api/checkout -rps 100
```

---

## Uso

```
carga -url <endpoint> [flags]
```

### Flags

| Flag | Tipo | Padrão | Descrição |
|---|---|---|---|
| `-url` | string | `http://localhost:8080/api/checkout` | Endpoint alvo do teste |
| `-method` | string | `POST` | Método HTTP (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`) |
| `-body` | string | — | Body da requisição em JSON literal. Omitir usa o payload padrão `{"user_id": N, "action": "checkout"}` para POST/PUT/PATCH. GET e outros métodos sem body não enviam nada. |
| `-rps` | int | `100` | Usuários virtuais injetados por segundo |
| `-duration` | duration | `30s` | Tempo total de execução do teste |
| `-timeout` | duration | `5s` | Deadline rígido por requisição (evita goroutines zumbi) |
| `-token` | string | — | Bearer token → `Authorization: Bearer <token>` |
| `-basic` | string | — | Basic auth no formato `usuario:senha` |
| `-H` | string | — | Header HTTP customizado no formato `Chave: Valor` (repetível) |

#### Formatos aceitos para `-duration` e `-timeout`

A flag aceita qualquer valor que Go interpreta como `time.Duration`:

```
30s     → 30 segundos
2m      → 2 minutos
1m30s   → 1 minuto e 30 segundos
500ms   → 500 milissegundos
```

---

## Exemplos

### Teste básico (GET)

```bash
carga -url http://localhost:8080/clubes -method GET
```

Executa 30 segundos a 100 RPS com GET, sem body.

### POST com payload padrão

```bash
carga -url http://localhost:8080/api/checkout
```

Envia `{"user_id": N, "action": "checkout"}` com `N` incrementando a cada requisição.

### POST com payload customizado

```bash
carga -url http://api.staging.internal/v2/order \
      -body '{"item_id": 99, "qty": 1}' \
      -rps 200
```

O mesmo JSON é enviado em todas as requisições.

### Aumentar RPS e duração

```bash
carga -url http://api.staging.internal/v2/order -rps 500 -duration 2m
```

500 usuários virtuais por segundo durante 2 minutos.

### Com Bearer token (JWT, OAuth2, etc.)

```bash
carga -url https://api.prod.example.com/checkout \
      -token eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9... \
      -rps 200 \
      -duration 1m
```

O header `Authorization: Bearer <token>` é adicionado em todas as requisições.

### Com Basic Auth

```bash
carga -url http://internal-api/endpoint \
      -basic admin:minha_senha_secreta \
      -rps 50
```

### Com headers customizados (multi-tenant, versionamento, etc.)

```bash
carga -url http://api.example.com/checkout \
      -token eyJ... \
      -H "X-Tenant: acme-corp" \
      -H "X-API-Version: 2" \
      -H "X-Request-Source: load-test" \
      -rps 300 \
      -duration 5m
```

A flag `-H` pode ser repetida quantas vezes for necessário.

### Timeout agressivo para detectar degradação

```bash
carga -url http://api.example.com/slow-endpoint \
      -rps 100 \
      -timeout 500ms \
      -duration 60s
```

Qualquer requisição que demore mais de 500 ms é contabilizada como `timeout` no relatório.

### Interromper antes do tempo

Pressione `Ctrl+C` a qualquer momento. A injeção para imediatamente, as requisições em voo são drenadas e o relatório final é impresso normalmente.

---

## Relatório de saída

Durante o teste, o progresso é exibido a cada 5 segundos:

```
[carga] starting  url=http://api.example.com/checkout  rps=200  duration=1m0s  timeout=5s  tick=5ms  vu/tick=1
[carga] header    Authorization: Bearer eyJ...
[carga] Ctrl+C stops injection early and still prints the report.
[carga]    5s elapsed — injected: 1000 VUs
[carga]   10s elapsed — injected: 2000 VUs
...
[carga] injection ended (1m0.001s) — waiting for 43 goroutines to drain...
```

Ao final, o relatório completo:

```
══════════════════════════════════════════════════════════════════
  carga — Load Test Report
══════════════════════════════════════════════════════════════════
  URL:                   http://api.example.com/checkout
  Duration:              1m0.001s
  Target RPS:            200 req/s
  Timeout:               5s
──────────────────────────────────────────────────────────────────
  THROUGHPUT
  Total requests:        12000
  Successful:            11943  (99.53%)
  Actual RPS:            199.98 req/s
──────────────────────────────────────────────────────────────────
  LATENCY
  min:                   812 µs
  p50  (median):         3.21 ms
  p95:                   18.40 ms
  p99:                   87.20 ms
  p99.9:                 412.00 ms
  max:                   1.823 s
  mean:                  5.10 ms
──────────────────────────────────────────────────────────────────
  ERRORS
  timeout:               34
  http_500:              23
══════════════════════════════════════════════════════════════════
```

### Interpretando os resultados

| Métrica | Significado |
|---|---|
| **Actual RPS** | Taxa real observada. Deve ser próxima do `-rps` configurado. Se for muito menor, o servidor está rejeitando conexões. |
| **p50 (median)** | Metade das requisições respondeu abaixo desse tempo. |
| **p95** | 95% das requisições responderam abaixo desse tempo. Bom indicador de experiência do usuário. |
| **p99** | Só 1% das requisições foi mais lento que isso. Indica cauda longa. |
| **p99.9** | O pior 0,1%. Revela comportamentos patológicos do servidor (GC pause, pool starvation, etc.). |
| **timeout** | Requisições que excederam o `-timeout` configurado. |
| **http_NNN** | Contagem por código de status HTTP de erro (ex: `http_500`, `http_503`). |
| **connection_error** | Falhas de transporte: recusa de conexão, reset TCP, DNS falhou. |

---

## Pool de conexões

O `http.Transport` é configurado automaticamente com base no `-rps` informado:

```
MaxIdleConns        = rps × 2
MaxIdleConnsPerHost = rps × 2
MaxConnsPerHost     = 0  (sem limite — o OS e o servidor aplicam backpressure)
```

O dimensionamento garante que conexões TCP ociosas fiquem disponíveis para reuso sem causar *socket exhaustion*, mesmo em picos de carga.

---

## Payload enviado

Cada usuário virtual envia um `POST` com `Content-Type: application/json` e o seguinte body:

```json
{
  "user_id": 42,
  "action": "checkout"
}
```

O `user_id` é um contador atômico global — cada requisição recebe um valor único e incremental durante toda a execução do teste.

---

## Limitações conhecidas

- **Método fixo:** todas as requisições usam `POST`. Suporte a `GET` e outros métodos pode ser adicionado com a flag `-method`.
- **Payload fixo:** o body JSON não é parametrizável via CLI. Para payloads dinâmicos, edite a função `runVU` no código-fonte.
- **Sem ramp-up:** a injeção começa imediatamente no RPS alvo. Para simular aquecimento gradual, execute múltiplas instâncias em sequência com RPS crescente.
- **Sem suporte a HTTP/2:** o `Transport` usa HTTP/1.1 por padrão. Para HTTP/2 remova o campo `DisableCompression` e defina `ForceAttemptHTTP2: true`.

---

## Dependências

| Pacote | Versão | Uso |
|---|---|---|
| [`github.com/HdrHistogram/hdrhistogram-go`](https://github.com/HdrHistogram/hdrhistogram-go) | v1.1.2 | Histograma de alta resolução para cálculo de percentis |

Todas as demais importações são da biblioteca padrão do Go.
