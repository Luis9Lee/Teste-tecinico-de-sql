# 📊 Pipeline ETL Censo: Rendimento e Desigualdade

Este repositório documenta um pipeline de Engenharia de Dados que processa dados brutos de um Censo Demográfico para calcular indicadores de Rendimento Médio Mensal. O projeto é um exemplo prático da Arquitetura Medallion implementada em um ambiente PostgreSQL (ELT) e consultada via PySpark/Databricks.

## 1\. Contexto, Tecnologia e Metodologia do Projeto

### 1.1. Motivação e Objetivos

O principal objetivo deste projeto é transformar dados complexos e sujos de um Censo em uma estrutura analítica limpa. A finalidade é permitir que analistas de BI e Ciência de Dados gerem *insights* rápidos sobre **disparidade de rendimento** e **comportamento demográfico** (incluindo variáveis como Sexo, Cor/Raça e, em análises secundárias, Meios de Transporte). O pipeline garante que a base de consumo final seja imutável e confiável.

### 1.2. Stack Tecnológico

| Ferramenta | Uso no Projeto |
| :--- | :--- |
| **PostgreSQL** | Motor central do ELT, gerenciamento de schemas e persistência das camadas Bronze, Silver e Gold. |
| **PySpark / Databricks** | Ambiente de desenvolvimento para validação de dados (Auditoria) e consultas analíticas exploratórias (ad-hoc). |
| **SQL (ANSI)** | Linguagem primária para todas as transformações de dados entre as camadas (limpeza, tipagem, agregação). |
| **Markdown / Git** | Documentação e controle de versão do pipeline. |

### 1.3. Abordagem Metodológica

1.  **Arquitetura Medallion (Bronze/Silver/Gold):** Estratégia adotada para promover a qualidade e rastreabilidade do dado. O dado evolui da forma bruta (Bronze) até o formato de BI (Gold).
2.  **Star Schema (Camada Gold):** Modelagem dimensional utilizada na camada de consumo. A Tabela Fato (`fato_indicadores`) centraliza os indicadores de rendimento, e as dimensões (`dim_categorias`, `dim_geografia`) fornecem o contexto necessário, simplificando as consultas de BI.
3.  **Cálculo de Rendimento:** A transformação crítica de `renda_total_nominal / populacao_ocupada` é feita na camada **Silver** com tratamento obrigatório de divisão por zero (`NULLIF(populacao_ocupada, 0)`), garantindo que a Camada Gold só receba valores válidos ou `NULL` (em vez de erros de execução).

-----

## 2\. Pipeline ELT em PostgreSQL (Detalhado)

### 2.1. Configuração e Ingestão BRONZE

```sql
-- Criação dos Schemas (As três camadas)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Criação da Tabela BRONZE (Entrada de Dados Brutos)
DROP TABLE IF EXISTS bronze.censo CASCADE;
CREATE TABLE bronze.censo (
    source_file TEXT, area_km2 TEXT, densidade_demografica_hab_km2 TEXT, 
    renda_total_nominal TEXT, populacao_ocupada TEXT, sexo TEXT TEXT, cor_ou_raca TEXT
);

-- 2. População com Dados de Exemplo
INSERT INTO bronze.censo (source_file, area_km2, densidade_demografica_hab_km2, renda_total_nominal, populacao_ocupada, sexo, cor_ou_raca) VALUES
-- ... [Dados de Exemplo Omitidos por Brevidade]
```

### 2.2. Transformação SILVER (Limpeza e Cálculo Crítico)

```sql
-- Cria a tabela SILVER, realizando a limpeza de tipos e o cálculo do rendimento.
DROP TABLE IF EXISTS silver.censo_consolidado_silver CASCADE;
-- ... [Código SILVER Omitido por Brevidade]
```

### 2.3. Modelagem GOLD (Criação do Star Schema e Agregação)

```sql
-- Criação de Dimensões, Tabela Fato e Agregação Final
-- ... [Código GOLD Omitido por Brevidade]
```

-----

## 3\. Qualidade de Dados e Insights (PySpark/Databricks)

Esta seção foca nas rotinas diárias de **Auditoria** e **Análise**, utilizando PySpark sobre os dados limpos (Silver) e agregados (Gold).

### 3.1. Funcionalidades de Qualidade e Auditoria

As funções de auditoria são vitais para a sustentação do pipeline, garantindo que as regras de ETL foram aplicadas corretamente:

  * **`check_null_counts`:** Rastreia a presença de valores nulos nas colunas críticas (rendimento, população).
  * **`check_uniqueness`:** Valida a integridade da chave composta (`id_geografia`, `sexo`, `cor_ou_raca`) na camada Silver, evitando duplicação de métricas.

### 3.2. Funcionalidades de Análise (Business Value)

As funções de análise exploram os *insights* de desigualdade presentes nos dados:

  * **`rank_rendimento_by_sex`:** Permite identificar rapidamente os top N municípios com maior rendimento para uma determinada categoria (ex: Homens ou Mulheres), direcionando a análise de poder aquisitivo.
  * **`calculate_gender_gap`:** Executa uma análise crucial de disparidade, calculando a diferença absoluta e a **razão** de rendimento (Homem / Mulher) por geografia. Este *insight* é fundamental para políticas de equidade salarial.

### 3.3. Notebook PySpark (Exemplo de Rotina)

```python
from pyspark.sql.functions import col, avg, round, min, max, count, when, lit, desc, countDistinct, abs
from pyspark.sql.window import Window

# --- 1. FUNÇÕES DE QUALIDADE E AUDITORIA AVANÇADA ---

def check_null_counts(df, columns):
    """Auditoria: Contagem de valores NULL em colunas."""
    df.select([count(when(col(c).isNull(), c)).alias(f"NULOS_{c}") for c in columns]).show()

def check_uniqueness(df, column):
    """Auditoria: Verifica a unicidade de uma chave."""
    # ... [Código Omitido por Brevidade]

# --- 2. FUNÇÕES DE ANÁLISE AVANÇADA ---

def rank_rendimento_by_sex(df_silver, category='Homens'):
    """Análise: Ranqueia as geografias pelo Rendimento Médio de uma categoria específica (uso de Window Function)."""
    # ... [Código Omitido por Brevidade]

def calculate_gender_gap(df_silver):
    """Análise: Calcula a disparidade de rendimento (homens vs. mulheres) por município e identifica o maior GAP."""
    print("\n--- Análise: Maiores Gaps de Rendimento por Município ---")
    
    df_pivot = df_silver.groupBy("id_geografia") \
        .pivot("sexo") \
        .agg(round(avg(col("rendimento_medio_mensal")), 2).alias("rendimento_medio")) \
        .filter(col("Homens").isNotNull() & col("Mulheres").isNotNull())

    df_gap = df_pivot.withColumn(
        "rendimento_gap", 
        col("Homens") - col("Mulheres")
    ).withColumn(
        "razao_homem_mulher", 
        round(col("Homens") / col("Mulheres"), 2)
    )

    df_gap.orderBy(desc("rendimento_gap")) \
          .select("id_geografia", "Homens", "Mulheres", "rendimento_gap", "razao_homem_mulher") \
          .show(3, truncate=False)
```
