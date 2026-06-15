# Orion

Ferramenta de teste de carga e benchmark HTTP escrita em Go puro, seguindo o **Open Model** de injeção de usuários do [Gatling](https://gatling.io/docs/gatling/reference/current/core/injection/#open-model).

---

## O que é o Open Model?

A maioria das ferramentas de teste de carga usa o **Closed Model**: um pool fixo de workers que só envia uma nova requisição após receber a resposta da anterior. Isso significa que, quando o servidor desacelera, a taxa de requisições por segundo também cai — e você acaba medindo *throughput* em vez de *pressão real*.

O **Open Model** funciona diferente: um `time.Ticker` dispara em intervalos fixos e **sempre** injeta um novo usuário virtual (goroutine), independentemente de quantas requisições ainda estão em voo. Se o servidor demorar 2 segundos para responder, as goroutines acumulam — mas a taxa de injeção é mantida. É assim que usuários reais se comportam.

```
Closed Model                    Open Model (Orion)
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
                    │  METHOD /endpoint         │
                    │  context.WithTimeout      │
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
cd Orion
go mod tidy
go build -o orion .
```

Ou execute diretamente sem compilar:

```bash
go run . -url http://localhost:8080/api/checkout -rps 100
```

---

## Uso

```
orion -url <endpoint> [flags]
```

### Flags

| Flag | Tipo | Padrão | Descrição |
|---|---|---|---|
| `-url` | string | `http://localhost:8080/api/checkout` | Endpoint alvo do teste |
| `-method` | string | `GET` | Método HTTP (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`) |
| `-body` | string | — | Body da requisição em JSON literal. Omitir usa o payload padrão `{"user_id": N, "action": "checkout"}` para POST/PUT/PATCH. GET e outros métodos sem body não enviam nada. |
| `-rps` | int | `100` | Usuários virtuais injetados por segundo |
| `-duration` | duration | `30s` | Tempo total de execução do teste |
| `-timeout` | duration | `5s` | Deadline rígido por requisição (evita goroutines zumbi) |
| `-token` | string | — | Bearer token → `Authorization: Bearer <token>` |
| `-basic` | string | — | Basic auth no formato `usuario:senha` |
| `-H` | string | — | Header HTTP customizado no formato `Chave: Valor` (repetível) |
| `-dashboard` | bool | `false` | Abre o dashboard ao vivo no browser durante o teste |
| `-dashboard-port` | int | `9191` | Porta local do servidor do dashboard |

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
orion -url http://localhost:8080/clubes -method GET
```

Executa 30 segundos a 100 RPS com GET, sem body.

### POST com payload padrão

```bash
orion -url http://localhost:8080/api/checkout
```

Envia `{"user_id": N, "action": "checkout"}` com `N` incrementando a cada requisição.

### POST com payload customizado

```bash
orion -url http://api.staging.internal/v2/order \
      -body '{"item_id": 99, "qty": 1}' \
      -rps 200
```

O mesmo JSON é enviado em todas as requisições.

### Aumentar RPS e duração

```bash
orion -url http://api.staging.internal/v2/order -rps 500 -duration 2m
```

500 usuários virtuais por segundo durante 2 minutos.

### Ramp-up gradual

```bash
orion -url http://api.staging.internal/v2/order \
      -rps 1000 \
      -ramp-up 30s \
      -duration 90s
```

Sobe linearmente de 0 até 1000 RPS nos primeiros 30 s, depois mantém a taxa pelos 60 s restantes. Útil para aquecimento de pool de conexões e JVM warm-up.

### Com Bearer token (JWT, OAuth2, etc.)

```bash
orion -url https://api.prod.example.com/checkout \
      -token eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9... \
      -rps 200 \
      -duration 1m
```

O header `Authorization: Bearer <token>` é adicionado em todas as requisições.

### Com Basic Auth

```bash
orion -url http://internal-api/endpoint \
      -basic admin:minha_senha_secreta \
      -rps 50
```

### Com headers customizados (multi-tenant, versionamento, etc.)

```bash
orion -url http://api.example.com/checkout \
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
orion -url http://api.example.com/slow-endpoint \
      -rps 100 \
      -timeout 500ms \
      -duration 60s
```

Qualquer requisição que demore mais de 500 ms é contabilizada como `timeout` no relatório.

### Com dashboard ao vivo

```bash
orion -url http://api.example.com/checkout -rps 200 -duration 2m -dashboard
```

Abre automaticamente `http://localhost:9191/` no browser com métricas em tempo real. Ao final do teste o botão **View HTML Report** aparece no próprio dashboard.

Para usar outra porta (ex: se 9191 já estiver ocupada):

```bash
orion -url http://api.example.com/checkout -rps 200 -dashboard -dashboard-port 8888
```

### Interromper antes do tempo

Pressione `Ctrl+C` a qualquer momento. A injeção para imediatamente, as requisições em voo são drenadas e o relatório final é impresso normalmente.

---

## Relatório de saída

Durante o teste, o progresso é exibido a cada 5 segundos:

```
[orion] starting  GET http://api.example.com/clubes  rps=200  duration=1m0s  timeout=5s  tick=5ms  vu/tick=1
[orion] header    Authorization: Bearer eyJ...
[orion] Ctrl+C stops injection early and still prints the report.
[orion]    5s elapsed — injected: 1000 VUs
[orion]   10s elapsed — injected: 2000 VUs
...
[orion] injection ended (1m0.001s) — waiting for 43 goroutines to drain...
```

Ao final, o relatório completo:

```
══════════════════════════════════════════════════════════════════
  Orion — Load Test Report
══════════════════════════════════════════════════════════════════
  URL:                   GET http://api.example.com/checkout
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
                         value            reqs ≥ value   reqs = value
  min:                   812 µs           12000          87
  p50  (median):         3.21 ms          6000           1240
  p95:                   18.40 ms         600            89
  p99:                   87.20 ms         120            14
  p99.9:                 412.00 ms        12             2
  max:                   1.823 s          1              1
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
| **reqs ≥ value** | Quantidade aproximada de requisições que demoraram pelo menos aquele tempo (calculado como `total × (1 − percentil/100)`). |
| **reqs = value** | Quantidade exata de requisições que caíram no mesmo bucket do HDR Histogram para aquela latência. Útil para saber o peso real de cada ponto na distribuição. |
| **timeout** | Requisições que excederam o `-timeout` configurado. |
| **http_NNN** | Contagem por código de status HTTP de erro (ex: `http_500`, `http_503`). |
| **connection_error** | Falhas de transporte: recusa de conexão, reset TCP, DNS falhou. |

O relatório HTML gerado automaticamente em `reports/` exibe as mesmas quatro colunas (Percentile, Latency, Requests ≥ value, Requests = value) na tabela **Latency Distribution**, além dos gráficos de throughput e latência ao longo do tempo.

---

## Dashboard ao vivo

Adicione `-dashboard` a qualquer comando para abrir uma interface web em tempo real durante o teste.

```bash
orion -url http://api.example.com/checkout -rps 200 -duration 2m -dashboard
```

O browser abre automaticamente em `http://localhost:9191/`. As métricas são enviadas via **Server-Sent Events (SSE)** — sem WebSocket, sem polling.

### O que é exibido

**Cards de resumo** (atualizam a cada segundo):

| Card | Descrição |
|---|---|
| Total Requests | Requisições acumuladas desde o início |
| Success Rate | Percentual de sucesso — verde ≥ 99%, amarelo ≥ 95%, vermelho abaixo disso |
| Current RPS | Requisições enviadas no último segundo |
| Avg RPS | Média de RPS desde o início do teste |
| Errors | Total de erros acumulados |
| p95 Latency | Latência do percentil 95 da janela atual (ms) |
| p99 Latency | Latência do percentil 99 da janela atual (ms) |
| Elapsed | Tempo decorrido desde o início |

**Gráficos** (janela deslizante de 90 segundos):
- **Throughput (req/s)** — success/s (verde) e errors/s (vermelho)
- **Latency Percentiles (ms)** — p50, p95 e p99 ao longo do tempo

**Painel lateral:**
- Endpoint/cenário alvo
- Modo (RPS ou VUs de browser)
- Duração configurada
- Status do relatório HTML (pending → ready)

### Ciclo de vida

```
teste inicia → dashboard abre no browser → métricas chegam via SSE a cada 1s
     ↓
teste termina → badge muda para "done" → relatório HTML é gerado
     ↓
botão "View HTML Report" aparece → servidor fica ativo por 2 min → shutdown automático
```

Se o browser for aberto depois do início do teste, a conexão recebe todo o histórico de snapshots de uma vez (catch-up) e então continua recebendo atualizações ao vivo.

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

## Payload padrão

Quando `-method` é `POST`, `PUT` ou `PATCH` e `-body` não é fornecido, o Orion gera automaticamente:

```json
{
  "user_id": 42,
  "action": "checkout"
}
```

O `user_id` é um contador atômico global — cada requisição recebe um valor único e incremental durante toda a execução do teste. Para payloads customizados, use a flag `-body`.

---

## Ambiente de execução e alto RPS

### Serviços rodando no Colima / Docker Desktop

Quando o serviço alvo está em um container local (Colima, Docker Desktop), `localhost` aponta para o Mac — não para a VM. Use o IP da VM diretamente:

```bash
# Colima
colima status   # mostra o endereço, normalmente 192.168.64.2
orion -url http://192.168.64.2:8080/clubes -method GET -rps 100

# Docker Desktop no Mac expõe as portas via localhost normalmente
```

### Limite de RPS em ambientes virtualizados

O Colima adiciona três camadas de virtualização entre o Orion e o container:

```
Orion (macOS) → NAT virtual → VM Linux → docker bridge → container
```

Na prática, isso limita o throughput útil a **~1.000–2.000 RPS** antes de o gargalo ser a rede virtualizada, não o serviço. Para testes acima disso, use uma das abordagens abaixo.

**Opção 1 — compilar para Linux e rodar dentro da VM (zero overhead de rede):**

```bash
GOOS=linux GOARCH=arm64 go build -o orion-linux-arm64 .
colima ssh -- /tmp/orion-linux-arm64 -url http://localhost:8080/clubes -rps 5000 -duration 60s
```

**Opção 2 — rodar contra staging/produção diretamente:**

```bash
orion -url https://api.internal.meli.com/clubes \
      -token eyJ... \
      -rps 10000 \
      -duration 60s
```

Elimina toda a virtualização local e mede latência real de rede.

### Diagnóstico: todas as requisições retornam HTTP 500

Se o navegador consegue acessar o endpoint mas o Orion retorna 100 % de `http_500`, o servidor está recebendo as requisições mas falta algum header obrigatório. Use o DevTools (F12 → Network → clique na requisição) para comparar os headers, ou copie como cURL (botão direito → "Copy as cURL") e identifique o que está faltando.

Headers mais comuns de adicionar:

```bash
orion -url http://localhost:8080/clubes -method GET \
      -token eyJhbGci...               \   # Bearer token
      -H "X-Tenant: mla"               \   # header customizado
      -H "Accept: application/json"
```

---

## Limitações conhecidas

- **Payload estático com `-body`:** o JSON passado via `-body` é enviado idêntico em todas as requisições. Para payloads dinâmicos por requisição, edite a função `runVU` no código-fonte.
- **Sem suporte a HTTP/2:** o `Transport` usa HTTP/1.1 por padrão. Para HTTP/2, defina `ForceAttemptHTTP2: true` no `http.Transport` em `buildClient()`.

---

## Dependências

| Pacote | Versão | Uso |
|---|---|---|
| [`github.com/HdrHistogram/hdrhistogram-go`](https://github.com/HdrHistogram/hdrhistogram-go) | v1.1.2 | Histograma de alta resolução para cálculo de percentis |

Todas as demais importações são da biblioteca padrão do Go.
