# PG Semantic Schema

A Clojure project that builds a semantic data pipeline to automatically convert tabular CSV data into PostgreSQL star/snowflake schemas using Apache Jena for RDF reasoning and ontology discovery.

## Features

- **Semantic Type Detection**: Automatically detects email addresses, phone numbers, currency values, dates, URLs, and other semantic patterns
- **RDF Conversion**: Converts CSV data to RDF triples with semantic annotations
- **Ontology Discovery**: Uses Apache Jena reasoners to discover relationships, hierarchies, and functional dependencies
- **Schema Pattern Detection**: Identifies star and snowflake schema patterns in data
- **PostgreSQL DDL Generation**: Generates complete DDL with proper data types, constraints, and indexes
- **REPL-Friendly Development**: Rich set of development utilities for exploration and testing

## Architecture

The pipeline consists of several key namespaces:

- `core.clj` - Main pipeline orchestration
- `rdf-conversion.clj` - CSV to RDF conversion with type detection
- `ontology-discovery.clj` - Semantic pattern discovery using Jena reasoners
- `schema-discovery.clj` - Star/snowflake schema pattern detection via SPARQL
- `sql-generation.clj` - PostgreSQL DDL generation from ontology
- `utils.clj` - Advanced semantic type detection utilities
- `config.clj` - Configuration and Jena setup

## Quick Start

### Prerequisites

- Java 8 or higher
- Leiningen 2.9 or higher

### Installation

```bash
git clone https://github.com/your-username/pg-semantic-schema.git
cd pg-semantic-schema
lein deps
```

### Basic Usage

#### Command Line

```bash
# Analyze a CSV file and generate PostgreSQL schema
lein run input.csv output.sql

# With custom table and schema names
lein run input.csv output.sql --table-name sales_data --schema-name analytics
```

#### Programmatic Usage

```clojure
(require '[pg-semantic-schema.core :as core])

;; Run the complete pipeline
(core/run-semantic-pipeline 
  "resources/sample-data/sales_fact.csv"
  "output/sales_schema.sql"
  :table-name "sales_fact"
  :schema-name "analytics")

;; Quick analysis for exploration
(core/analyze-csv "data.csv" :sample-size 100)
```

### REPL Development

Start a REPL session for interactive development:

```bash
lein repl
```

The `dev/user.clj` namespace provides rich development utilities:

```clojure
;; Load development functions
(dev-help)

;; Quick analysis of a CSV file
(quick-analyze "resources/sample-data/sales_fact.csv")

;; Inspect detected semantic types
(inspect-semantic-types)

;; View relationship discoveries
(inspect-relationships)

;; See schema pattern recommendations
(inspect-schema-recommendation)

;; Generate DDL preview
(generate-ddl-preview :schema-name "sales_dw")

;; Test semantic type detection
(test-semantic-detection ["john@example.com" "$1,234.56" "2023-12-25"])
```

## Examples

### Sales Fact Table Analysis

```clojure
;; Analyze sales data
(quick-analyze "resources/sample-data/sales_fact.csv")

;; Expected output:
{:file "resources/sample-data/sales_fact.csv"
 :analysis-time-ms 450
 :rdf-triples 847
 :semantic-types 6
 :relationships 4
 :recommended-pattern :star
 :pattern-confidence 0.9
 :column-count 10}
```

### Generated PostgreSQL Schema

The pipeline automatically generates:

- **Fact tables** with surrogate keys and foreign key references
- **Dimension tables** with SCD Type 2 support
- **Proper data types** based on semantic analysis
- **Indexes** for performance optimization
- **Comments** documenting semantic types

Example generated DDL:

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Fact table
CREATE TABLE analytics.semantic_sales_fact_fact (
    fact_id BIGSERIAL PRIMARY KEY,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(15,2) NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    customer_id_key BIGINT,
    product_id_key BIGINT,
    sales_date_key BIGINT,
    CONSTRAINT fk_semantic_sales_fact_fact_customer_id_key 
        FOREIGN KEY (customer_id_key) REFERENCES analytics.dim_customer_id(dim_id)
);

-- Indexes for performance
CREATE INDEX idx_semantic_sales_fact_fact_measures 
    ON analytics.semantic_sales_fact_fact (quantity, unit_price, total_amount);

-- Table comments
COMMENT ON TABLE analytics.semantic_sales_fact_fact 
    IS 'Auto-generated table from semantic analysis';
```

## Semantic Type Detection

The system detects over 20 semantic types:

| Type | Example | PostgreSQL Type |
|------|---------|----------------|
| Email | `john@example.com` | `VARCHAR(255)` |
| Phone | `(555) 123-4567` | `VARCHAR(20)` |
| Currency | `$1,234.56` | `DECIMAL(15,2)` |
| Date | `2023-12-25` | `DATE` |
| URL | `https://example.com` | `TEXT` |
| ZIP Code | `12345-6789` | `VARCHAR(10)` |
| SSN | `123-45-6789` | `CHAR(11)` |
| UUID | `550e8400-e29b-41d4-a716-446655440000` | `UUID` |

## Configuration

Customize behavior through configuration:

```clojure
(def custom-config
  {:jena {:reasoner-type :owl-micro
          :namespace-base "http://example.org/schema/"}
   :csv {:delimiter \,
         :sample-size 1000}
   :postgres {:schema-prefix "semantic_"
              :fact-table-suffix "_fact"}
   :semantic-types {:confidence-threshold 0.8}})

(core/run-semantic-pipeline "data.csv" "output.sql" :config custom-config)
```

## Sample Data

The project includes sample datasets:

- `sales_fact.csv` - Sales transaction data (fact table example)
- `customer_dimension.csv` - Customer information (dimension table example)
- `product_hierarchy.csv` - Product catalog with hierarchies (snowflake example)
- `employee_data.csv` - Employee records with sensitive data types

## Schema Patterns

### Star Schema
- Central fact table with measures
- Dimension tables with foreign key references
- Optimized for query performance

### Snowflake Schema  
- Normalized dimension hierarchies
- Reduced data redundancy
- More complex join patterns

### Pattern Detection
The system analyzes:
- Column cardinality and uniqueness
- Functional dependencies
- Hierarchical relationships  
- Semantic type distributions

## Development

### Running Tests

```bash
lein test
```

### Building

```bash
# Create standalone JAR
lein uberjar

# Run the JAR
java -jar target/uberjar/pg-semantic-schema-0.1.0-SNAPSHOT-standalone.jar input.csv output.sql
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Technical Details

### RDF Processing
- Uses Apache Jena for RDF model creation and reasoning
- Converts tabular data to semantic triples
- Applies reasoning rules to discover implicit relationships

### Ontology Discovery
- Functional dependency detection through co-occurrence analysis
- Hierarchical relationship discovery via subset analysis
- Cardinality inference based on uniqueness patterns

### Schema Generation
- Surrogate key generation for fact and dimension tables
- SCD Type 2 support for dimension tables
- Automatic index creation for performance
- Data type mapping from semantic types to PostgreSQL

## Performance

Typical performance on modern hardware:
- 1,000 rows: ~200ms
- 10,000 rows: ~800ms  
- 100,000 rows: ~3.5s

Memory usage scales with data size and reasoning complexity.

## Limitations

- Currently supports CSV input format only
- PostgreSQL DDL generation only
- English-language semantic patterns
- Limited to tabular data structures

## License

Copyright © 2024

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

## Related Projects

- [Apache Jena](https://jena.apache.org/) - RDF processing and reasoning
- [Stardog](https://www.stardog.com/) - Knowledge graph platform
- [Apache Spark](https://spark.apache.org/) - Large-scale data processing
- [Great Expectations](https://greatexpectations.io/) - Data quality validation