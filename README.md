# prospectar-engine

Motor de scraping do **Prospectar**, a feature de geração de leads do
[Orbita](https://github.com/murichristopher/orbita). Raspa o **Bing Maps** com um
Chromium headless e entrega os leads de duas formas: em tempo real por SSE ou
por uma fila assíncrona que grava direto no Mongo do Orbita.

Escrito em **Rust** (`axum` + `chromiumoxide` + `tokio`). É um port fiel do
motor em Go que vivia em `orbita/scraper/`: mesma API HTTP, mesmos eventos,
mesmas coleções no Mongo. O Orbita troca de motor sem mudar uma linha — basta
apontar `SCRAPER_URL` para este serviço.

A arquitetura interna, o contrato de dados e as armadilhas do port estão em
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Rodar local

Pré-requisitos: Rust 1.88+ e Google Chrome (ou Chromium).

```bash
cp .env.example .env   # opcional; as variáveis também podem ir na linha de comando

# macOS: aponta para o Chrome instalado
CHROME_PATH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  cargo run --release

# em outra aba
curl -N "http://localhost:3001/scrape?q=nutricionista+em+Sao+Paulo&maxPages=3"
```

Sem `MONGODB_URI` só o modo SSE (`GET /scrape`) fica ativo. Para testar a fila
assíncrona sem tocar no banco de produção, suba um Mongo descartável:

```bash
docker run -d --name prospectar-mongo -p 27099:27017 mongo:7
MONGODB_URI=mongodb://localhost:27099 MONGODB_DB=prospectar_test \
CHROME_PATH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  cargo run --release
```

> **Locale:** o efeito de `--lang=pt-BR` depende do build do Chrome. Com o
> Chrome desktop (macOS, ou o Chrome completo do Ubuntu) as categorias do Bing
> vêm em inglês ("Nutritionist"). Na imagem Docker de produção, com o
> `headless-shell`, vêm em português ("Nutricionista") — o mesmo comportamento
> do motor Go. Para validar dados em português, teste pela imagem Docker.

## Docker

```bash
docker build -t prospectar-engine .
docker run --rm -p 3001:3001 --shm-size=1g \
  -e MONGODB_URI="mongodb+srv://..." \
  prospectar-engine
```

A imagem usa `chromedp/headless-shell` como base (Chromium mínimo, Debian 13)
e já define `CHROME_PATH`. O binário Rust tem ~17 MB; quase todo o tamanho da
imagem (~550 MB) é o Chromium. `--shm-size=1g` evita crashes de aba por falta
de memória compartilhada. O container encerra limpo com `docker stop`: fecha o
Chromium, apaga o perfil temporário e sai com código 0.

## Testes

```bash
CHROME_PATH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  cargo test                     # 66 testes

cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Sem `CHROME_PATH`, os 4 testes que sobem o Chromium retornam cedo e aparecem
como aprovados sem ter rodado (avisam no stderr). Para validar o browser,
rode sempre com `CHROME_PATH` definido. O CI roda com o Chrome do runner
Ubuntu, então lá eles executam de verdade.

Os testes cobrem o parsing do `data-entity` do Bing (inclusive o duplo-encode),
o contrato JSON/BSON dos eventos e leads, a montagem da URL do Bing, a
normalização e deduplicação de lugares, o geocoder (contra um servidor HTTP
local), o tiling geográfico, a fila (cancelamento, fila cheia, timeout) e todas
as rotas HTTP. Os testes de browser verificam regressões específicas do port:
WebGL disponível, flags chegando ao Chrome sem mesclagem (lidas da linha de
comando real via CDP), perfil isolado por launch e liberação do slot de
concorrência.

A navegação real no Bing não é testada em unidade — o DOM do Bing muda sem
aviso. Ela é validada por smoke com `curl` (ver
[Validação de paridade](docs/ARCHITECTURE.md#validação-de-paridade-com-o-motor-go)).

## Variáveis de ambiente

| variável               | default                                      | descrição |
|------------------------|----------------------------------------------|-----------|
| `PORT`                 | `3001`                                       | porta HTTP |
| `ALLOWED_ORIGIN`       | `http://localhost:3000`                      | origem do CORS (só para chamadas diretas do browser) |
| `CHROME_PATH`          | auto-detecção                                | binário do Chrome/Chromium |
| `SCRAPE_CONCURRENCY`   | `1`                                          | scrapes simultâneos — **não suba sem proxy**, o Bing degrada |
| `MONGODB_URI`          | —                                            | habilita o `POST /scrape` assíncrono |
| `MONGODB_DB`           | `orbita`                                     | banco das coleções `scrapesessions`/`scrapeleads` |
| `JOB_TIMEOUT_MINUTES`  | `30`                                         | tempo máximo de um job assíncrono |
| `NOMINATIM_URL`        | `https://nominatim.openstreetmap.org/search` | geocoder dos jobs por área |
| `NOMINATIM_USER_AGENT` | `orbita-prospectar/1.0 (+https://axolutions.com.br)` | exigido pela política do Nominatim |
| `RUST_LOG`             | `info`                                       | nível de log (`debug` mostra falhas de JS na página) |
| `SCRAPER_DEBUG`        | —                                            | qualquer valor liga logs da paginação |

## API

Todas as respostas de erro têm o formato `{"error": "<mensagem>"}`.

### `GET /health`

`200 {"ok":true}`.

### `GET /scrape` — stream SSE

Raspa uma busca e devolve os eventos em tempo real. A conexão fica aberta até o
evento final. Se o cliente desconectar, a aba do Chromium é fechada na hora.

| param       | obrigatório | descrição |
|-------------|-------------|-----------|
| `q`         | sim         | termo da busca |
| `lat`,`lng` | não         | centro do mapa (só valem juntos) |
| `zoom`      | não         | nível de zoom (default 12 quando há coordenadas) |
| `maxPages`  | não         | limite de páginas; default 10, teto 50 |

Erros antes do stream: `400` (`q` ausente, `maxPages` inválido) e `500`
(browser indisponível).

Eventos, um por linha `data: <json>`:

```
{"type":"lead","data":{...}}
{"type":"progress","page":1,"count":24}
{"type":"done","total":109}
{"type":"error","message":"..."}
```

O formato do lead é contrato com o Orbita (`ILead` em `src/lib/types.ts` lá) —
ver [Contrato de dados](docs/ARCHITECTURE.md#contrato-de-dados).

### `POST /scrape` — job assíncrono

Recebe um job já criado pelo Orbita em `scrapesessions` e responde na hora; o
worker raspa em background e escreve direto no Mongo. Requer `MONGODB_URI`.

```json
{
  "jobId": "<ObjectId hex do scrapesessions>",
  "query": "dentista",
  "lat": -23.55, "lng": -46.63, "zoom": 12,
  "maxPages": 10,
  "location": "São Paulo, SP",
  "targetLeads": 100,
  "cellKm": 3
}
```

`jobId` e `query` são obrigatórios. Com `location`, o job vira uma **varredura
por área**: o local é geocodificado e dividido em células, raspadas do centro
para fora até atingir `targetLeads` (default 100). Sem `location`, é um scrape
de ponto único em `lat`/`lng`. `maxPages` vale por célula na varredura por área.

| status | quando |
|--------|--------|
| `202 {"jobId":"...","status":"queued"}` | aceito |
| `400`  | JSON inválido, ou `jobId`/`query` ausentes |
| `503`  | sem `MONGODB_URI`, ou fila cheia (100 jobs) |

### `DELETE /scrape/{jobId}` — cancelamento

`200 {"jobId":"...","running":true|false}`. `running` diz se o job estava
executando; um job ainda na fila também é cancelado e nunca roda. O job
termina com `status: "cancelled"` preservando os leads já coletados.
`503` sem `MONGODB_URI`.

## Migrando do motor Go

1. Publique a imagem deste repositório onde o `scraper.axolutions.com.br` roda
   hoje, com as mesmas variáveis de ambiente do serviço Go.
2. Valide com `curl https://<host>/health` e um `GET /scrape` de teste.
3. Aponte `SCRAPER_URL` do Orbita para o novo host (ou troque o container
   atrás do mesmo domínio).

Nada muda no Orbita: rotas, payloads, eventos e documentos no Mongo são
idênticos — ver o comparativo em
[Validação de paridade](docs/ARCHITECTURE.md#validação-de-paridade-com-o-motor-go).
