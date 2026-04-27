# Pipeline Medallion — Olist E-Commerce

Pipeline de dados seguindo a arquitetura Medallion (Bronze → Silver → Gold) usando PySpark + Delta Lake, processando o dataset público Brazilian E-Commerce da Olist.

---

## Diagrama da arquitetura

| Camada | Script | Entrada | Transformações | Saída |
|--------|--------|---------|----------------|-------|
| **Bronze** | `01_bronze.py` | 7 CSVs em `data/raw/` | Nenhuma — dados brutos + coluna `ingestion_timestamp` | `delta/bronze/orders`, `order_items`, `customers`, `products`, `sellers`, `payments`, `reviews` |
| **Silver** | `02_silver.py` | Tabelas Bronze (Delta) | Remove nulos (`order_id`, `customer_id`), deduplica por `order_id`, filtra `delivered`/`shipped`, converte datas para `TimestampType`, join entre orders + items + customers + products + sellers, agrega pagamentos por `order_id` | `delta/silver/orders_consolidated`, `delta/silver/payments_summary` |
| **Gold** | `03_gold.py` | Tabelas Silver + Bronze reviews (Delta) | Agregações analíticas por cliente (`customer_id`, categoria de produto e vendedor, com receita = `price + freight_value` | `delta/gold/customer_summary`, `delta/gold/product_summary`, `delta/gold/seller_summary` |
| **Share** | `04_share_simulation.py` | Tabelas Gold (Delta) | Export para CSV + resumo executivo no console | `output/gold_*_export.csv` |

---

## Decisões de design

### 1. Filtro de status na Silver (não na Bronze)

Descartei pedidos com status `canceled`, `unavailable`, `invoiced`, `processing`, `approved` e `created`, mantendo apenas `delivered` e `shipped`. Essa decisão fica na Silver porque a Bronze deve preservar os dados brutos — se em outro momento precisarmos analisar cancelamentos, por exemplo, os dados continuam disponíveis na camada de ingestão.

### 2. Deduplicação por `order_id` com `dropDuplicates`

Usei `dropDuplicates(["order_id"])` para eliminar pedidos duplicados. É a abordagem mais direta e suficiente para este caso. Uma alternativa seria `row_number()` com window function pra manter o registro mais recente, mas aqui não há critério claro de "mais recente" nos dados — o `dropDuplicates` resolve sem complexidade extra.

### 3. Receita calculada como `price + freight_value`

A receita total inclui o frete porque ele é pago pelo cliente e representa o valor real da transação. Essa soma é feita na Gold no momento da agregação, sem criar colunas intermediárias na Silver que poderiam causar confusão.

### 4. Agregação de pagamentos antes do join

A tabela `payments` tem múltiplas linhas por pedido (parcelas, vouchers). Agreguei por `order_id` na Silver (`payments_summary`) antes de qualquer join, evitando fan-out que inflaria valores de receita nas tabelas Gold.

### 5. `customer_unique_id` no customer_summary

O `customer_id` muda entre pedidos do mesmo comprador. Usei `customer_unique_id` como chave de agrupamento no `customer_summary` pra ter uma visão real de clientes únicos, não de contas. Ao final, renomeei a coluna para `customer_id` para seguir o que foi solicitado no enunciado do teste.

---

## Como rodar o projeto

### Pré-requisitos

- Docker instalado

### Passo a passo

1. Clone o repositório:

```bash
git clone https://github.com/victoriars-it/teste-tecnico-eng-dados-jr-rethink.git
cd teste-tecnico-eng-dados-jr-rethink
```

2. Baixe o dataset [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) no Kaggle e extraia os CSVs dentro de `data/raw/`:

```
data/raw/olist_orders_dataset.csv
data/raw/olist_order_items_dataset.csv
data/raw/olist_customers_dataset.csv
data/raw/olist_products_dataset.csv
data/raw/olist_sellers_dataset.csv
data/raw/olist_order_payments_dataset.csv
data/raw/olist_order_reviews_dataset.csv
```

3. Builde a imagem Docker:

```bash
docker build -t teste-tecnico .
```

4. Execute o pipeline completo:

```bash
docker run --rm -v "${PWD}:/app" teste-tecnico python pipeline_runner.py
```

Ou execute cada etapa individualmente:

```bash
docker run --rm -v "${PWD}:/app" teste-tecnico python 01_bronze.py
docker run --rm -v "${PWD}:/app" teste-tecnico python 02_silver.py
docker run --rm -v "${PWD}:/app" teste-tecnico python 03_gold.py
```

As tabelas Delta são geradas em `delta/bronze/`, `delta/silver/` e `delta/gold/`.

5. Para rodar a simulação de Delta Sharing (export CSV + resumo executivo):

```bash
docker run --rm -v "${PWD}:/app" teste-tecnico python 04_share_simulation.py
```

Os CSVs exportados ficam em `output/`.


---

## O que mudaria em produção

1. **Orquestração com Azure Data Factory:** em vez do `pipeline_runner.py` chamando scripts via subprocess, cada etapa seria uma atividade no ADF com dependências explícitas, retries configuráveis e alertas de falha.

2. **Delta Sharing com personal token:** as tabelas Gold seriam compartilhadas via Delta Sharing em vez de exportar CSVs. Consumidores (dashboards, outros times) acessariam os dados diretamente sem cópia, com controle de acesso via RBAC e Unity Catalog.

3. **Otimização de storage:** em produção usaríamos `OPTIMIZE` e `Z-ORDER` nas tabelas Delta para compactar small files e otimizar leituras por colunas frequentes de filtro (ex: `order_purchase_timestamp`, `customer_state`). Também habilitaríamos Auto Optimize no Databricks.

4. **Processamento incremental:** o pipeline atual faz overwrite completo a cada execução. Em produção, usaríamos merge/upsert com Delta (`MERGE INTO`) para processar apenas dados novos, reduzindo custo e tempo.

5. **Qualidade de dados:** integraríamos Great Expectations ou Deequ para validações automáticas (ex: `order_id` nunca nulo, `payment_value >= 0`) com bloqueio de pipeline em caso de violação.

---

## Limitações

- **Sem processamento incremental:** toda execução reprocessa os dados do zero. Funciona para o volume do Olist (~100k registros) mas não escalaria sem merge incremental.
- **Sem validação formal de qualidade:** os filtros de nulo e status estão no código, mas não há framework de validação (Great Expectations/Deequ) com relatórios de qualidade.
- **Reviews com múltiplas avaliações por pedido:** o dataset pode ter mais de uma review por `order_id`. O join com reviews na Gold pode inflar a contagem — idealmente seria tratado com agregação prévia (ex: média por `order_id`) antes do join.
- **Categorias em português:** `product_category_name` está em português no dataset original. A tabela de tradução existe no Kaggle mas não foi usada, já que o enunciado lista apenas 7 CSVs.
- **Sem testes automatizados:** o pipeline não tem testes unitários. Com mais tempo, validaria schemas de saída e contagens esperadas por camada.