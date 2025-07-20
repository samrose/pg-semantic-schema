# Usage Examples

This document provides comprehensive examples of using the PG Semantic Schema pipeline.

## Table of Contents

1. [Basic Pipeline Usage](#basic-pipeline-usage)
2. [REPL Development Workflow](#repl-development-workflow)
3. [Advanced Configuration](#advanced-configuration)
4. [Semantic Type Detection](#semantic-type-detection)
5. [Schema Pattern Analysis](#schema-pattern-analysis)
6. [Batch Processing](#batch-processing)
7. [Integration Examples](#integration-examples)

## Basic Pipeline Usage

### Command Line Interface

```bash
# Basic usage - analyze a CSV and generate PostgreSQL schema
lein run resources/sample-data/sales_fact.csv output/sales_schema.sql

# With custom names
lein run data/customers.csv schemas/customer_dim.sql \
  --table-name customers \
  --schema-name warehouse

# Process all CSV files in a directory
find data/ -name "*.csv" -exec lein run {} schemas/{}.sql \;
```

### Programmatic Usage

```clojure
(ns my-analysis
  (:require [pg-semantic-schema.core :as core]
            [pg-semantic-schema.config :as config]))

;; Basic pipeline execution
(core/run-semantic-pipeline 
  "data/sales.csv"
  "output/sales_schema.sql")

;; With custom configuration
(core/run-semantic-pipeline 
  "data/sales.csv"
  "output/sales_schema.sql"
  :table-name "sales_transactions"
  :schema-name "analytics"
  :config (assoc-in config/*default-config* [:csv :sample-size] 5000))

;; Quick analysis without file output
(def analysis (core/analyze-csv "data/sales.csv"))
(println "Detected" (:semantic-types analysis) "semantic types")
```

## REPL Development Workflow

### Starting Development Session

```clojure
;; Start REPL
lein repl

;; Load development utilities
(dev-help)  ; Shows all available functions

;; Set logging level for detailed output
(set-log-level! :debug)
```

### Exploring Data

```clojure
;; Quick analysis of sample data
(quick-analyze "resources/sample-data/sales_fact.csv")
;; => {:file "resources/sample-data/sales_fact.csv"
;;     :rdf-triples 847
;;     :semantic-types 6
;;     :recommended-pattern :star
;;     :pattern-confidence 0.9}

;; Analyze all sample files
(analyze-samples)
;; => [{:filename "sales_fact.csv" :recommended-pattern :star}
;;     {:filename "customer_dimension.csv" :recommended-pattern :dimension-table}
;;     ...]

;; Inspect detected semantic types
(inspect-semantic-types)
;; => [{:property "email" :type :email :confidence 0.95}
;;     {:property "phone" :type :phone :confidence 0.89}
;;     {:property "sales_date" :type :date-iso :confidence 0.98}]
```

### Relationship Discovery

```clojure
;; View discovered relationships
(inspect-relationships)

;; Output:
;; === Foreign Key Candidates ===
;; [{:from-column "customer_id" :to-column "customer_id" :similarity 0.87}
;;  {:from-column "product_id" :to-column "product_id" :similarity 0.92}]
;;
;; === Hierarchies ===  
;; [{:parent-column "category" :child-column "subcategory" :coverage 0.95}]
;;
;; === Functional Dependencies ===
;; [{:determinant "order_id" :dependent "order_date" :strength 1.0}]

;; Check column classifications
(inspect-column-roles)
;; => [{:column "order_id" :role :identifier :uniqueness 1.0}
;;     {:column "quantity" :role :measure :semantic-type nil}
;;     {:column "customer_id" :role :dimension :uniqueness 0.2}]
```

### Schema Recommendations

```clojure
;; View schema pattern recommendations
(inspect-schema-recommendation)

;; Output:
;; === Schema Pattern Recommendation ===
;; Pattern: :star
;; Confidence: 0.9
;; 
;; Recommendations:
;;  - Add surrogate key (auto-incrementing ID) as primary key
;;  - Configure 3 measure columns with appropriate aggregation functions
;;  - Create foreign key relationships to dimension tables
;;  - Create composite indexes on foreign key combinations

;; Generate DDL preview
(generate-ddl-preview :schema-name "sales_dw" :table-name "sales_fact")

;; Output:
;; CREATE SCHEMA IF NOT EXISTS sales_dw;
;; 
;; CREATE TABLE sales_dw.semantic_sales_fact_fact (
;;     fact_id BIGSERIAL PRIMARY KEY,
;;     quantity BIGINT NOT NULL,
;;     unit_price DECIMAL(15,2) NOT NULL,
;;     ...
;; );
```

## Advanced Configuration

### Custom Semantic Patterns

```clojure
(def custom-config
  (-> config/*default-config*
      (assoc-in [:semantic-types :patterns :product-code] 
                #"^[A-Z]{2}\d{4}$")
      (assoc-in [:semantic-types :patterns :isbn]
                #"^\d{3}-\d{10}$")))

(quick-analyze "data/products.csv" custom-config)
```

### Database-Specific Configuration

```clojure
(def postgres-config
  (-> config/*default-config*
      (assoc-in [:postgres :schema-prefix] "warehouse_")
      (assoc-in [:postgres :fact-table-suffix] "_facts")
      (assoc-in [:postgres :dim-table-suffix] "_dimensions")))

(core/run-semantic-pipeline 
  "data/sales.csv" 
  "schemas/sales.sql"
  :config postgres-config)
```

### Performance Tuning

```clojure
(def performance-config
  (-> config/*default-config*
      (assoc-in [:csv :sample-size] 10000)  ; Larger sample
      (assoc-in [:jena :reasoner-type] :rdfs)  ; Lighter reasoning
      (assoc-in [:semantic-types :confidence-threshold] 0.7)))  ; Lower threshold

;; Benchmark different configurations
(benchmark-pipeline "large-data.csv" :iterations 5)
;; => {:iterations 5 :average-ms 2340 :min-ms 2100 :max-ms 2890}
```

## Semantic Type Detection

### Testing Custom Patterns

```clojure
;; Test semantic detection on sample values
(test-semantic-detection 
  ["john.doe@company.com"
   "(555) 123-4567"
   "$1,234.56"
   "2023-12-25"
   "https://example.com"
   "550e8400-e29b-41d4-a716-446655440000"])

;; => [{:value "john.doe@company.com" :detected-type :email :confidence 0.9}
;;     {:value "(555) 123-4567" :detected-type :phone :confidence 0.85}
;;     {:value "$1,234.56" :detected-type :currency :confidence 0.9}
;;     {:value "2023-12-25" :detected-type :date-iso :confidence 0.95}
;;     {:value "https://example.com" :detected-type :url :confidence 0.9}
;;     {:value "550e8400-..." :detected-type :uuid :confidence 0.95}]
```

### Analyzing Column Patterns

```clojure
(require '[pg-semantic-schema.utils :as utils])

;; Analyze patterns in a column
(def email-column ["john@example.com" "jane@test.org" "bob@company.net" ""])
(utils/analyze-column-patterns email-column)

;; => {:semantic-type :email
;;     :semantic-confidence 1.0
;;     :uniqueness-ratio 1.0
;;     :null-ratio 0.25
;;     :string-patterns {:min-length 13 :max-length 17 :has-special-chars true}}
```

## Schema Pattern Analysis

### Comparing Different Patterns

```clojure
;; Compare star vs snowflake patterns for the same data
(compare-patterns "resources/sample-data/product_hierarchy.csv")

;; Output:
;; === Pattern Comparison ===
;; star:
;;   Confidence: 0.7
;;   Recommendations: 4
;;    - Add surrogate key as primary key
;;    - Configure 2 measure columns
;;
;; snowflake:
;;   Confidence: 0.85
;;   Recommendations: 6
;;    - Normalize dimension hierarchies into separate tables
;;    - Consider impact on query performance
```

### Manual Pattern Override

```clojure
;; Force a specific pattern regardless of confidence
(defn force-star-pattern [csv-file]
  (let [analysis (quick-analyze csv-file)
        schema-analysis (:schema-analysis *last-analysis*)
        forced-pattern (assoc-in schema-analysis 
                                [:recommended-pattern :schema-pattern] 
                                :star)]
    (sql/generate-ddl forced-pattern "forced_table" "forced_schema" config/*default-config*)))
```

## Batch Processing

### Directory Processing

```clojure
;; Process all CSV files in a directory
(core/run-pipeline-from-directory "input/" "output/" :parallel? true)

;; With custom configuration
(core/run-pipeline-from-directory 
  "data/raw/" 
  "schemas/"
  :config custom-config
  :parallel? false)  ; Sequential for debugging
```

### Custom Batch Logic

```clojure
(defn process-data-warehouse-files []
  (let [fact-tables ["sales_fact.csv" "order_fact.csv" "returns_fact.csv"]
        dim-tables ["customer_dim.csv" "product_dim.csv" "time_dim.csv"]]
    
    ;; Process dimension tables first
    (doseq [dim-file dim-tables]
      (core/run-semantic-pipeline 
        (str "data/" dim-file)
        (str "schemas/dimensions/" dim-file ".sql")
        :schema-name "dimensions"))
    
    ;; Process fact tables
    (doseq [fact-file fact-tables]
      (core/run-semantic-pipeline 
        (str "data/" fact-file)
        (str "schemas/facts/" fact-file ".sql")
        :schema-name "facts"))))
```

## Integration Examples

### With Database Migration Tools

```clojure
;; Generate Flyway migration files
(defn generate-flyway-migration [csv-file version description]
  (let [timestamp (java.time.LocalDateTime/now)
        filename (format "V%s__%s.sql" version (clojure.string/replace description " " "_"))
        ddl-statements (core/run-semantic-pipeline csv-file "/tmp/temp.sql")]
    
    (spit (str "db/migration/" filename)
          (str "-- Flyway migration generated from " csv-file "\n"
               "-- Description: " description "\n"
               "-- Generated: " timestamp "\n\n"
               (slurp "/tmp/temp.sql")))))

(generate-flyway-migration "sales.csv" "001" "Create sales fact table")
```

### With Data Pipeline Tools

```clojure
;; Apache Airflow DAG integration
(defn create-airflow-task [csv-file output-file]
  {:task_id (str "analyze_" (clojure.string/replace csv-file #"[^a-zA-Z0-9]" "_"))
   :bash_command (str "cd /opt/pg-semantic-schema && "
                     "lein run " csv-file " " output-file)
   :depends_on_past false
   :retries 1})

;; Generate DAG definition
(defn generate-dag [csv-files]
  (let [tasks (map #(create-airflow-task % (str "output/" % ".sql")) csv-files)]
    {:dag_id "semantic_schema_pipeline"
     :default_args {:owner "data-team"
                   :start_date "2024-01-01"}
     :schedule_interval "@daily"
     :tasks tasks}))
```

### With Testing Frameworks

```clojure
(ns my-project.semantic-tests
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.core :as core]))

(deftest test-sales-schema-generation
  (testing "Sales fact table generates star schema"
    (let [result (core/analyze-csv "test/data/sales_sample.csv")]
      (is (= :star (:recommended-pattern result)))
      (is (> (:pattern-confidence result) 0.8))
      (is (>= (:semantic-types result) 5)))))

(deftest test-semantic-type-coverage
  (testing "Employee data detects PII types"
    (let [result (core/analyze-csv "test/data/employees.csv")]
      (is (contains? (set (map :type (:detected-types result))) :email))
      (is (contains? (set (map :type (:detected-types result))) :ssn)))))
```

## Error Handling and Debugging

### Common Issues and Solutions

```clojure
;; Enable detailed logging
(set-log-level! :debug)

;; Handle parse errors
(try
  (quick-analyze "problematic-file.csv")
  (catch Exception e
    (println "Analysis failed:" (.getMessage e))
    (clojure.stacktrace/print-stack-trace e)))

;; Check for encoding issues
(defn analyze-with-encoding [csv-file encoding]
  (let [config (assoc-in config/*default-config* [:csv :encoding] encoding)]
    (quick-analyze csv-file config)))

;; Validate semantic detection
(defn validate-detection [csv-file expected-types]
  (let [analysis (quick-analyze csv-file)
        detected-types (set (map :type (:semantic-types *last-analysis*)))]
    (every? detected-types expected-types)))
```

### Performance Debugging

```clojure
;; Profile different components
(defn profile-pipeline [csv-file]
  (let [start-time (System/currentTimeMillis)]
    
    (println "Starting RDF conversion...")
    (let [rdf-start (System/currentTimeMillis)
          model (rdf/csv->rdf csv-file config/*default-config*)
          rdf-time (- (System/currentTimeMillis) rdf-start)]
      
      (println "RDF conversion took:" rdf-time "ms")
      (println "Starting ontology discovery...")
      
      (let [onto-start (System/currentTimeMillis)
            ontology (onto/discover-ontology model config/*default-config*)
            onto-time (- (System/currentTimeMillis) onto-start)]
        
        (println "Ontology discovery took:" onto-time "ms")
        {:rdf-time rdf-time
         :ontology-time onto-time
         :total-time (- (System/currentTimeMillis) start-time)}))))
```

This completes the comprehensive usage documentation for the PG Semantic Schema project.