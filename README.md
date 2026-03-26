# tap-inriver

[Singer](https://www.singer.io/) tap for the **inRiver PIM** REST API, built with the Hotglue Singer SDK.

Streams:

- **Product** — incremental on `modified_date` (query + batched `entities:fetchdata`)
- **ProductItem** — Item rows with `product_id` (from Product → Item links)
- **ItemSize** — Size rows with `item_id` (from Item → Size links)

## Requirements

- Python 3.9–3.12

## Install

```bash
pip install -e .
# or: poetry install
```

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `api_url_base` | yes | Environment base URL (e.g. `https://api-test1a-use.productmarketingcloud.com`) |
| `api_key` | yes | REST API key (`X-inRiver-APIKey`) |
| `start_date` | no | ISO 8601 lower bound for Product `LastModified` query |
| `batch_size` | no | Products per batch (default `25`) |

## Usage

Discover catalog:

```bash
tap-inriver --config config.json --discover > catalog.json
```

Sync (example):

```bash
tap-inriver --config config.json --catalog catalog.json > out.singer
```

Point `config.json` at your credentials; keep secrets out of version control.

## API examples (curl)

See **[docs/curl-roller-skates.md](docs/curl-roller-skates.md)** for REST examples aligned with **`inriver-swagger.json`**: create Product / Item / Size, then `ProductItem` and `ItemSize` links (e.g. “Roller Skates” with colors and sizes). Plain copy-paste list: **[docs/curl-roller-skates.txt](docs/curl-roller-skates.txt)**.

## License

Apache-2.0
