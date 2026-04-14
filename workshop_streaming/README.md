# WORKSHOP PRÁTICO: Data Streaming Local com Kafka, Parquet e Iceberg (Medallion Architecture)

Material guiado para implementação local de um pipeline de streaming usando o dataset de aeroportos do [OpenFlights](https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat).

**Objetivo:** Publicar dados em partes no Kafka, consumir em micro-batches de 30 segundos, persistir a Bronze em Parquet, transformar a Silver em Apache Iceberg e gerar uma Gold somente com aeroportos do Brasil.

---

## 1. Visão geral do workshop

Neste workshop, nós vamos construir um pipeline de dados ponta a ponta rodando localmente. A ideia é mostrar o funcionamento de streaming de forma didática, sem esconder ou abstrair a arquitetura da aplicação.

### O que nós vamos implementar:
- **Producer Python:** Lê o dataset externo de aeroportos.
- **Envio fragmentado:** Envia eventos em blocos (chunks) para um tópico no Kafka.
- **Consumer Bronze:** Trabalha através de micro-batches baseados em tempo (30s).
- **Persistência Bronze (Parquet):** Arquivamento do dado bruto chegado do Kafka em arquivos `.parquet`.
- **Transformação Silver (Iceberg):** Lê a camada Bronze apontando tipos, unindo esquemas e removendo duplicações utilizando o Apache Iceberg via PySpark.
- **Curadoria Gold (Iceberg):** Tabela derivada da camada Silver contendo apenas as informações valiosas e filtradas para consumo final (neste caso, Aeroportos do Brasil).

### Conceitos abordados:
- Producer, Topic, Consumer Group, Offsets.
- Micro-batch guiado por relógio (tempo) e commits manuais.
- A arquitetura Medalhão: Camadas Bronze, Silver e Gold.
- Schema, tipagem correta, lidando com nulos e deduplicação de chaves.
- Parquet enquanto base estruturada crua vs formato de tabelas transacionais e analíticas Apache Iceberg.

---

## 2. Arquitetura do exercício

```text
OpenFlights airports.dat
        |
        v
producer.py
  - lê o dataset da web
  - envia para o Kafka em pequenos blocos
        |
        v
Kafka topic: airports_raw
        |
        v
consumer_bronze.py
  - consome mensagens em janela de 30 segundos
  - grava batch em formato Parquet dentro de `data/bronze/airports`
        |
        v
silver_iceberg.py
  - lê o Parquet da camada Bronze via PySpark
  - trata schemas e tipos
  - grava Iceberg Catalog Table em `local.silver.airports`
        |
        v
gold_brazil.py
  - lê os Dataframes a partir da Silver
  - filtra por country == 'Brazil'
  - escreve de volta no Iceberg como `local.gold.airports_br`
```

---

## 3. Pré-requisitos

| Item | Uso no workshop |
| ---- | ----------------|
| **Python 3.10+** | Executar os scripts do pipeline |
| **Docker e Compose** | Orquestrar e subir o Cluster Kafka local (Kraft) |
| **Java 17+** | Requisito do Apache Spark (PySpark) para rodar o runtime do Iceberg |

---

## 4. Estrutura do projeto

```text
workshop_streaming/
├── docker-compose.yml       # Infraestrutura local para o Kafka (Kraft)
├── requirements.txt         # Pacotes Python
├── producer.py              # Script produtor Kafka
├── consumer_bronze.py       # Script consumidor (micro-batch) de Kafka -> Parquet
├── silver_iceberg.py        # PySpark: Parquet -> Bronze table in Iceberg
├── gold_brazil.py           # PySpark: Filtra Silver table -> Gold table
└── data/                    # Pasta autogerada que conterá a Bronze e o Warehouse (Iceberg)
```

---

## 5. Passos para Execução

### Etapa 0: Preparar Infraestrutura

Abra seu terminal no diretório do projeto e inicie o recruta Kafka em *background*. Ele rodará utilizando a porta local na 9092.

```bash
docker compose up -d
```

Em seguida, inicialize um ambiente virtual Python isolado e instale os pacotes:

```bash
python -m venv .venv

# Dependendo do SO, o comando ativador do VENV muda:
# Linux/Mac:
source .venv/bin/activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

---

### Etapa 1 - Producer enviando em partes

Aqui, o produtor será iniciado para capturar o dado externo com um atraso proposital de envio para conseguirmos ver a mecânica acontecendo ao vivo.

Em um terminal (com o `.venv` ativo), rode:
```bash
python producer.py
```
> **Dica:** O dataset contém mais de 7000 aeroportos e será inserido em "pedacinhos" com os contadores de `chunk_number` informando a posição atual do evento. Deixe-o executando livre.

---

### Etapa 2 - Consumer Bronze (Micro-batch 30s)

Em **outro terminal** (também com `.venv` ativo), acione nosso consumidor da camada Bronze. 
Ele lê registros durante uma janela corrida de 30 segundos, gera tabelas temporárias e dumpar os resultados em `/data/bronze/airports`, efetuando log de commits.

```bash
python consumer_bronze.py
```
> **Comportamento esperado:** A cada 30 segundos exatos, uma mensagem de `[Micro-batch 30s elapsed]` será descrita no terminal junto a um novo artefato de arquivo .parquet criado fisicamente em seu disco.

Deixe ambos coletando os dados durante alguns minutos ou até o final do loop principal e então, interrompa (`Ctrl+C`).

---

### Etapa 3 - Silver com Apache Iceberg

A camada Bronze possui dados sujos, sem formato definitivo e podendo causar duplicações. Na limpeza, faremos uso de PySpark rodando offline para parsear tudo em tabelas Apache Iceberg.

```bash
python silver_iceberg.py
```
*OBS: O Spark irá efetuar download de pacotes Maven automaticamente no primeiro uso.*

Na conclusão do processamento, observe as pastas instanciadas em `/data/warehouse/`.

---

### Etapa 4 - Gold (Aeroportos do Brasil)

Tendo a tabela base prontas na Silver, geramos facilmente um dataset altamente qualificado pela simples filtragem nas queries com PySpark.

```bash
python gold_brazil.py
```

Resultará em logs com o DataFrame visual e número total de pistas (aeroportos) presentes no Brasil.

---

## 6. Limpeza e Encerramento

Para desmontar o seu laboratório Kafka da máquina:

```bash
docker compose down
```

Para resetar ou reiniciar o tutorial do zero, exclua manualmente o diretório `./data` no projeto e inicie de onde preferir!
