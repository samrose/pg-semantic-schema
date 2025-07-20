(ns user
  "REPL-friendly development functions and utilities"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [clojure.tools.namespace.repl :refer [refresh]]
            [taoensso.timbre :as log]
            [pg-semantic-schema.core :as core]
            [pg-semantic-schema.config :as config]
            [pg-semantic-schema.rdf-conversion :as rdf]
            [pg-semantic-schema.ontology-discovery :as onto]
            [pg-semantic-schema.schema-discovery :as schema]
            [pg-semantic-schema.sql-generation :as sql]
            [pg-semantic-schema.utils :as utils])
  (:import [org.apache.jena.rdf.model ModelFactory]))

(def ^:dynamic *last-analysis* nil)
(def ^:dynamic *last-model* nil)

(defn set-log-level!
  "Set logging level for development"
  [level]
  (log/set-level! level)
  (log/info "Log level set to" level))

(defn quick-analyze
  "Quick analysis of a CSV file - great for REPL exploration"
  ([csv-file] (quick-analyze csv-file {}))
  ([csv-file options]
   (let [config (merge config/*default-config* options)
         start-time (System/currentTimeMillis)]
     
     (log/info "Starting quick analysis of" csv-file)
     
     (try
       (let [model (rdf/csv->rdf csv-file config)
             ontology (onto/discover-ontology model config)
             schema-analysis (schema/discover-schema-patterns model ontology config)
             
             duration (- (System/currentTimeMillis) start-time)
             
             summary {:file csv-file
                     :analysis-time-ms duration
                     :rdf-triples (.size model)
                     :semantic-types (count (:semantic-types ontology))
                     :relationships (+ (count (:foreign-key-candidates ontology))
                                     (count (:hierarchies ontology)))
                     :recommended-pattern (get-in schema-analysis [:recommended-pattern :schema-pattern])
                     :pattern-confidence (get-in schema-analysis [:recommended-pattern :pattern-confidence])
                     :column-count (count (:column-roles ontology))}]
         
         ;; Store for later inspection
         (alter-var-root #'*last-analysis* (constantly {:model model
                                                       :ontology ontology
                                                       :schema-analysis schema-analysis
                                                       :config config}))
         (alter-var-root #'*last-model* (constantly model))
         
         (log/info "Quick analysis completed in" duration "ms")
         summary)
       
       (catch Exception e
         (log/error e "Quick analysis failed")
         {:error (.getMessage e)})))))

(defn analyze-samples
  "Analyze all sample CSV files"
  []
  (let [sample-dir "resources/sample-data"
        csv-files (->> (io/file sample-dir)
                      file-seq
                      (filter #(str/ends-with? (.getName %) ".csv"))
                      (map #(.getAbsolutePath %)))]
    
    (log/info "Found" (count csv-files) "sample files")
    
    (mapv (fn [csv-file]
            (let [filename (.getName (io/file csv-file))]
              (log/info "Analyzing" filename)
              (assoc (quick-analyze csv-file {:csv {:sample-size 50}})
                     :filename filename)))
          csv-files)))

(defn inspect-semantic-types
  "Inspect semantic types detected in the last analysis"
  []
  (when-let [analysis *last-analysis*]
    (let [semantic-types (:semantic-types (:ontology analysis))]
      (pprint/pprint
       (mapv (fn [st]
               {:property (-> (:property st) (str/split #"/") last)
                :type (:semanticType st)
                :confidence (Double/parseDouble (:confidence st))})
             semantic-types)))))

(defn inspect-relationships
  "Inspect relationships discovered in the last analysis"
  []
  (when-let [analysis *last-analysis*]
    (let [ontology (:ontology analysis)]
      (println "\n=== Foreign Key Candidates ===")
      (pprint/pprint (:foreign-key-candidates ontology))
      
      (println "\n=== Hierarchies ===")
      (pprint/pprint (:hierarchies ontology))
      
      (println "\n=== Functional Dependencies ===")
      (pprint/pprint (:functional-dependencies ontology)))))

(defn inspect-column-roles
  "Inspect column role classifications"
  []
  (when-let [analysis *last-analysis*]
    (let [column-roles (:column-roles (:ontology analysis))]
      (pprint/pprint
       (mapv (fn [col]
               {:column (-> (:property col) (str/split #"/") last)
                :role (:role col)
                :semantic-type (:semantic-type col)
                :uniqueness (:uniqueness col)
                :completeness (:completeness col)})
             column-roles)))))

(defn inspect-schema-recommendation
  "Inspect schema pattern recommendation"
  []
  (when-let [analysis *last-analysis*]
    (let [recommendation (:recommended-pattern (:schema-analysis analysis))]
      (println "\n=== Schema Pattern Recommendation ===")
      (println "Pattern:" (:schema-pattern recommendation))
      (println "Confidence:" (:pattern-confidence recommendation))
      (println "\nRecommendations:")
      (doseq [rec (:recommendations recommendation)]
        (println " -" rec)))))

(defn generate-ddl-preview
  "Generate and preview DDL for the last analysis"
  [& {:keys [schema-name table-name]
      :or {schema-name "semantic_test"
           table-name "test_table"}}]
  (when-let [analysis *last-analysis*]
    (let [ddl-statements (sql/generate-ddl (:schema-analysis analysis)
                                          table-name
                                          schema-name
                                          (:config analysis))]
      (doseq [stmt ddl-statements]
        (println stmt)
        (println)))))

(defn test-semantic-detection
  "Test semantic type detection on sample values"
  [values]
  (mapv (fn [value]
          (let [detection (utils/detect-semantic-type-advanced value)]
            {:value value
             :detected-type (:type detection)
             :confidence (:confidence detection)
             :description (:description detection)}))
        values))

(defn benchmark-pipeline
  "Benchmark the full pipeline on a CSV file"
  [csv-file & {:keys [iterations] :or {iterations 3}}]
  (let [times (atom [])]
    (dotimes [i iterations]
      (let [start-time (System/currentTimeMillis)]
        (core/run-semantic-pipeline csv-file "/tmp/benchmark-output.sql")
        (swap! times conj (- (System/currentTimeMillis) start-time))))
    
    (let [avg-time (/ (reduce + @times) iterations)
          min-time (apply min @times)
          max-time (apply max @times)]
      {:iterations iterations
       :times @times
       :average-ms avg-time
       :min-ms min-time
       :max-ms max-time})))

(defn compare-patterns
  "Compare different schema patterns for the same data"
  [csv-file]
  (let [analysis (quick-analyze csv-file)]
    (when-let [last-analysis *last-analysis*]
      (let [patterns (:detected-patterns (:schema-analysis last-analysis))]
        (println "\n=== Pattern Comparison ===")
        (doseq [[pattern-type pattern-data] patterns]
          (when pattern-data
            (println (str (name pattern-type) ":"))
            (println "  Confidence:" (:pattern-confidence pattern-data))
            (when-let [recs (:recommendations pattern-data)]
              (println "  Recommendations:" (count recs))
              (doseq [rec (take 3 recs)]
                (println "   -" rec)))
            (println)))))))

(defn export-rdf
  "Export the last RDF model to a file"
  [filename & {:keys [format] :or {format "TURTLE"}}]
  (when-let [model *last-model*]
    (with-open [out (io/output-stream filename)]
      (.write model out format))
    (log/info "RDF model exported to" filename "in" format "format")))

(defn query-rdf
  "Execute a SPARQL query on the last RDF model"
  [query-string]
  (when-let [analysis *last-analysis*]
    (onto/execute-sparql-query (:model analysis) query-string)))

(defn reset-dev!
  "Reset development environment"
  []
  (alter-var-root #'*last-analysis* (constantly nil))
  (alter-var-root #'*last-model* (constantly nil))
  (refresh)
  (log/info "Development environment reset"))

(defn dev-help
  "Show available development functions"
  []
  (println "
=== PG Semantic Schema Development Functions ===

Analysis Functions:
  (quick-analyze \"file.csv\")          - Quick analysis of a CSV file
  (analyze-samples)                    - Analyze all sample CSV files
  (benchmark-pipeline \"file.csv\")     - Benchmark pipeline performance

Inspection Functions:
  (inspect-semantic-types)             - Show detected semantic types
  (inspect-relationships)              - Show discovered relationships
  (inspect-column-roles)               - Show column role classifications
  (inspect-schema-recommendation)      - Show schema pattern recommendation

Generation Functions:
  (generate-ddl-preview)               - Preview generated DDL
  (compare-patterns \"file.csv\")       - Compare different schema patterns

Testing Functions:
  (test-semantic-detection [\"val1\" \"val2\"]) - Test semantic type detection

RDF Functions:
  (export-rdf \"output.ttl\")           - Export RDF model to file
  (query-rdf \"SPARQL query\")          - Execute SPARQL query

Utility Functions:
  (set-log-level! :debug)              - Set logging level
  (reset-dev!)                         - Reset development environment
  (dev-help)                           - Show this help

Example workflow:
  (quick-analyze \"resources/sample-data/sales_fact.csv\")
  (inspect-semantic-types)
  (inspect-schema-recommendation)
  (generate-ddl-preview)
"))

(comment
  ;; Example REPL session
  
  ;; Start with help
  (dev-help)
  
  ;; Set debug logging
  (set-log-level! :debug)
  
  ;; Analyze a sample file
  (quick-analyze "resources/sample-data/sales_fact.csv")
  
  ;; Inspect the results
  (inspect-semantic-types)
  (inspect-relationships)
  (inspect-column-roles)
  (inspect-schema-recommendation)
  
  ;; Generate DDL
  (generate-ddl-preview :schema-name "sales_dw" :table-name "sales_fact")
  
  ;; Test semantic detection
  (test-semantic-detection ["john@example.com" "$1,234.56" "2023-12-25" "(555) 123-4567"])
  
  ;; Compare patterns
  (compare-patterns "resources/sample-data/product_hierarchy.csv")
  
  ;; Analyze all samples
  (analyze-samples)
  
  ;; Export RDF for external analysis
  (export-rdf "output.ttl")
  
  ;; Custom SPARQL query
  (query-rdf "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"))