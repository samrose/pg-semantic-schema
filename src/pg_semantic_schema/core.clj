(ns pg-semantic-schema.core
  "Main pipeline orchestration for semantic CSV to PostgreSQL schema conversion"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config]
            [pg-semantic-schema.rdf-conversion :as rdf]
            [pg-semantic-schema.ontology-discovery :as onto]
            [pg-semantic-schema.schema-discovery :as schema]
            [pg-semantic-schema.sql-generation :as sql]
            [pg-semantic-schema.naming :as naming])
  (:gen-class))

(defn validate-inputs
  "Validate pipeline inputs"
  [csv-file output-path]
  (when-not (.exists (io/file csv-file))
    (throw (ex-info "CSV file does not exist" {:file csv-file})))
  (let [output-file (io/file output-path)
        parent-dir (.getParent output-file)]
    (when (and parent-dir (not (.exists (io/file parent-dir))))
      (throw (ex-info "Output directory does not exist" {:path parent-dir})))))

(defn pipeline-step
  "Execute a pipeline step with logging and error handling"
  [step-name step-fn & args]
  (log/info (str "Starting " step-name "..."))
  (try
    (let [start-time (System/currentTimeMillis)
          result (apply step-fn args)
          duration (- (System/currentTimeMillis) start-time)]
      (log/info (str "Completed " step-name " in " duration "ms"))
      result)
    (catch Exception e
      (log/error e (str "Failed during " step-name))
      (throw e))))

(defn run-semantic-pipeline
  "Main pipeline function that orchestrates the semantic schema discovery process"
  [csv-file output-path & {:keys [config table-name schema-name]
                           :or {config config/*default-config*
                                table-name (-> csv-file
                                              io/file
                                              .getName
                                              (str/replace #"\.csv$" ""))
                                schema-name "semantic"}}]
  (validate-inputs csv-file output-path)
  
  (log/info "Starting semantic pipeline for" csv-file)
  (log/info "Configuration:" config)
  
  (let [;; Add table name to config
        enriched-config (assoc config :table-name table-name)
        
        ;; Step 1: Convert CSV to RDF with semantic type detection
        rdf-model (pipeline-step "CSV to RDF conversion"
                                rdf/csv->rdf
                                csv-file
                                enriched-config)
        
        ;; Step 2: Discover ontological patterns and relationships
        ontology-data (pipeline-step "Ontology discovery"
                                    onto/discover-ontology
                                    rdf-model
                                    enriched-config)
        
        ;; Step 3: Analyze for star/snowflake schema patterns
        schema-analysis (pipeline-step "Schema pattern discovery"
                                      schema/discover-schema-patterns
                                      rdf-model
                                      ontology-data
                                      enriched-config)
        
        ;; Step 4: Generate intelligent names if not provided
        intelligent-names (when (or (nil? table-name) (= schema-name "semantic"))
                           (naming/generate-intelligent-names 
                            ontology-data 
                            (:semantic-types ontology-data)
                            (or table-name (-> csv-file io/file .getName (str/replace #"\.csv$" "")))))
        
        ;; Use intelligent names or provided names
        final-table-name (or table-name (:table-name intelligent-names) 
                            (-> csv-file io/file .getName (str/replace #"\.csv$" "")))
        final-schema-name (if (= schema-name "semantic")
                           (:schema-name intelligent-names)
                           schema-name)
        
        ;; Log naming decisions
        _ (when intelligent-names
            (log/info "Intelligent naming applied:")
            (log/info "  Detected domain:" (:detected-domain intelligent-names))
            (log/info "  Table purpose:" (:table-purpose intelligent-names))
            (log/info "  Generated schema:" final-schema-name)
            (log/info "  Generated table:" final-table-name))
        
        ;; Step 5: Generate PostgreSQL DDL
        ddl-statements (pipeline-step "SQL DDL generation"
                                     sql/generate-ddl
                                     (assoc schema-analysis :ontology ontology-data)
                                     final-table-name
                                     final-schema-name
                                     enriched-config)
        
        ;; Create output structure
        output {:csv-file csv-file
                :table-name final-table-name
                :schema-name final-schema-name
                :intelligent-names intelligent-names
                :rdf-model rdf-model
                :ontology ontology-data
                :schema-analysis schema-analysis
                :ddl-statements ddl-statements
                :config enriched-config}]
    
    ;; Write DDL to file
    (spit output-path (str/join "\n\n" ddl-statements))
    (log/info "Pipeline completed successfully. DDL written to" output-path)
    
    output))

(defn run-pipeline-from-directory
  "Process all CSV files in a directory"
  [input-dir output-dir & {:keys [config parallel?]
                           :or {config config/*default-config*
                                parallel? false}}]
  (let [csv-files (->> (io/file input-dir)
                      file-seq
                      (filter #(str/ends-with? (.getName %) ".csv")))]
    (log/info "Found" (count csv-files) "CSV files to process")
    
    (let [process-fn (fn [csv-file]
                      (let [base-name (-> csv-file .getName (str/replace #"\.csv$" ""))
                            output-file (io/file output-dir (str base-name ".sql"))]
                        (run-semantic-pipeline (.getAbsolutePath csv-file)
                                             (.getAbsolutePath output-file)
                                             :config config
                                             :table-name base-name)))]
      (if parallel?
        (doall (pmap process-fn csv-files))
        (doall (map process-fn csv-files))))))

(defn analyze-csv
  "Quick analysis function for REPL exploration"
  [csv-file & {:keys [sample-size]
               :or {sample-size 100}}]
  (let [config (assoc-in config/*default-config* [:csv :sample-size] sample-size)
        rdf-model (rdf/csv->rdf csv-file config)
        ontology-data (onto/discover-ontology rdf-model config)]
    {:rdf-triples (.size rdf-model)
     :semantic-types (get-in ontology-data [:semantic-types])
     :relationships (get-in ontology-data [:relationships])
     :hierarchies (get-in ontology-data [:hierarchies])
     :functional-dependencies (get-in ontology-data [:functional-dependencies])}))

(defn -main
  "CLI entry point"
  [& args]
  (when (< (count args) 2)
    (println "Usage: lein run <csv-file> <output-file> [options]")
    (println "  csv-file    Path to input CSV file")
    (println "  output-file Path to output SQL file")
    (println "Options:")
    (println "  --table-name NAME    Override table name")
    (println "  --schema-name NAME   Override schema name")
    (System/exit 1))
  
  (let [[csv-file output-file & opts] args
        parsed-opts (apply hash-map opts)
        table-name (:table-name parsed-opts)
        schema-name (:schema-name parsed-opts)]
    (try
      (run-semantic-pipeline csv-file output-file
                           :table-name table-name
                           :schema-name (or schema-name "semantic"))
      (println "Pipeline completed successfully!")
      (catch Exception e
        (log/error e "Pipeline failed")
        (println "Pipeline failed:" (.getMessage e))
        (System/exit 1)))))