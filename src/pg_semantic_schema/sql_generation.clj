(ns pg-semantic-schema.sql-generation
  "PostgreSQL DDL generation from ontology analysis"
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config]
            [pg-semantic-schema.ontology-discovery :as onto])
  (:import [java.util UUID]))

(defn semantic-type->postgres-type
  "Map semantic types to appropriate PostgreSQL data types"
  [semantic-type data-type]
  (case (keyword semantic-type)
    :email "VARCHAR(255)"
    :phone "VARCHAR(20)"
    :currency "DECIMAL(15,2)"
    :date "DATE"
    :time "TIME"
    :url "TEXT"
    :zip-code "VARCHAR(10)"
    :ssn "CHAR(11)"
    
    ;; Fall back to XSD data type mapping
    (case (str data-type)
      "http://www.w3.org/2001/XMLSchema#integer" "BIGINT"
      "http://www.w3.org/2001/XMLSchema#decimal" "DECIMAL(15,4)"
      "http://www.w3.org/2001/XMLSchema#boolean" "BOOLEAN"
      "http://www.w3.org/2001/XMLSchema#date" "DATE"
      "http://www.w3.org/2001/XMLSchema#dateTime" "TIMESTAMP"
      "http://www.w3.org/2001/XMLSchema#time" "TIME"
      "TEXT")))

(defn calculate-varchar-length
  "Calculate appropriate VARCHAR length based on data analysis"
  [column-role unique-count semantic-type]
  (cond
    ;; Specific semantic types have known lengths
    (= semantic-type :email) 255
    (= semantic-type :phone) 20
    (= semantic-type :zip-code) 10
    (= semantic-type :ssn) 11
    
    ;; Role-based sizing
    (= column-role :identifier) (max 50 (min 255 (* unique-count 2)))
    (= column-role :categorical-dimension) (max 50 (min 100 unique-count))
    (= column-role :dimension) (max 100 (min 500 (* unique-count 3)))
    
    ;; Default sizing
    :else 255))

(defn generate-column-definition
  "Generate column definition for PostgreSQL"
  [column-data table-type config]
  (let [property-uri (or (:property column-data) "unknown_property")
        column-name (-> property-uri
                       (str/split #"/")
                       last
                       (str/replace #"[^a-zA-Z0-9_]" "_")
                       str/lower-case)
        
        semantic-type (keyword (:semantic-type column-data))
        role (:role column-data)
        unique-count (Integer/parseInt (onto/parse-rdf-literal (:uniqueValues column-data "0")))
        null-count (Integer/parseInt (onto/parse-rdf-literal (:nullValues column-data "0")))
        total-count (+ unique-count null-count)
        
        ;; Determine PostgreSQL data type
        postgres-type (semantic-type->postgres-type semantic-type (:data-type column-data))
        
        ;; Adjust VARCHAR lengths
        adjusted-type (if (str/starts-with? postgres-type "VARCHAR")
                       (str "VARCHAR(" 
                            (calculate-varchar-length role unique-count semantic-type) 
                            ")")
                       postgres-type)
        
        ;; Determine constraints - be more conservative about UNIQUE constraints
        not-null? (and (> total-count 0) (< (/ null-count total-count) 0.1)) ; Less than 10% nulls
        
        ;; Only apply UNIQUE constraint to true identifiers with specific patterns
        unique? (and (= role :identifier)
                     (> unique-count 5) ; Must have reasonable sample size
                     (or (.contains (str/lower-case column-name) "id")
                         (.contains (str/lower-case column-name) "key")
                         (.contains (str/lower-case column-name) "code")
                         (.contains (str/lower-case column-name) "number"))
                     (= semantic-type :unknown)) ; Don't make semantic types unique
        
        constraints (cond-> []
                      not-null? (conj "NOT NULL")
                      unique? (conj "UNIQUE"))]
    
    {:column-name column-name
     :data-type adjusted-type
     :constraints constraints
     :role role
     :semantic-type semantic-type
     :original-property property-uri}))

(defn generate-fact-table-ddl
  "Generate DDL for a fact table"
  [fact-structure schema-name config]
  (let [table-name (str (get-in config [:postgres :schema-prefix] "semantic_")
                       (:table-name fact-structure)
                       (get-in config [:postgres :fact-table-suffix] "_fact"))
        
        ;; Generate surrogate key
        surrogate-key {:column-name "fact_id"
                      :data-type "BIGSERIAL"
                      :constraints ["PRIMARY KEY"]
                      :role :surrogate-key}
        
        ;; Generate measure columns
        measure-columns (mapv #(generate-column-definition % :fact config) 
                             (:measures fact-structure))
        
        ;; Generate dimension foreign key columns
        dimension-fk-columns (mapv (fn [dim]
                                    (let [base-def (generate-column-definition dim :fact config)
                                          fk-name (str (:column-name base-def) "_key")]
                                      (assoc base-def 
                                             :column-name fk-name
                                             :data-type "BIGINT"
                                             :role :foreign-key
                                             :references-table (str "dim_" (:column-name base-def)))))
                                  (:dimension-references fact-structure))
        
        ;; Combine all columns
        all-columns (concat [surrogate-key] measure-columns dimension-fk-columns)
        
        ;; Generate column definitions
        column-defs (mapv (fn [col]
                           (str "    " (:column-name col) " " (:data-type col)
                                (when (seq (:constraints col))
                                  (str " " (str/join " " (:constraints col))))))
                         all-columns)
        
        ;; Generate foreign key constraints
        fk-constraints (mapv (fn [col]
                              (when (= (:role col) :foreign-key)
                                (str "    CONSTRAINT fk_" table-name "_" (:column-name col)
                                     " FOREIGN KEY (" (:column-name col) ")"
                                     " REFERENCES " schema-name "." (:references-table col) "(dim_id)")))
                            all-columns)
        
        ;; Generate indexes
        indexes [(str "CREATE INDEX idx_" table-name "_measures ON " schema-name "." table-name 
                     " (" (str/join ", " (map :column-name measure-columns)) ");")
                (str "CREATE INDEX idx_" table-name "_dimensions ON " schema-name "." table-name 
                     " (" (str/join ", " (map :column-name dimension-fk-columns)) ");")]
        
        ;; Main table DDL
        table-ddl (str "CREATE TABLE " schema-name "." table-name " (\n"
                      (str/join ",\n" (concat column-defs (filter some? fk-constraints)))
                      "\n);")]
    
    {:table-ddl table-ddl
     :indexes indexes
     :table-name table-name
     :columns all-columns}))

(defn generate-dimension-table-ddl
  "Generate DDL for a dimension table"
  [dim-structure schema-name config]
  (let [table-name (str "dim_" (str/replace (:table-name dim-structure) #"[^a-zA-Z0-9_]" "_"))
        
        ;; Generate surrogate key
        surrogate-key {:column-name "dim_id"
                      :data-type "BIGSERIAL" 
                      :constraints ["PRIMARY KEY"]
                      :role :surrogate-key}
        
        ;; Generate natural key column
        natural-key (when-let [nk (:natural-key dim-structure)]
                     (assoc (generate-column-definition nk :dimension config)
                            :role :natural-key
                            :constraints ["NOT NULL" "UNIQUE"]))
        
        ;; Generate attribute columns
        attribute-columns (mapv #(generate-column-definition % :dimension config)
                               (:attributes dim-structure))
        
        ;; SCD Type 2 columns
        scd-columns [{:column-name "effective_date"
                     :data-type "DATE"
                     :constraints ["NOT NULL" "DEFAULT CURRENT_DATE"]
                     :role :scd-metadata}
                    {:column-name "expiry_date"
                     :data-type "DATE"
                     :constraints ["DEFAULT '9999-12-31'"]
                     :role :scd-metadata}
                    {:column-name "is_current"
                     :data-type "BOOLEAN"
                     :constraints ["NOT NULL" "DEFAULT TRUE"]
                     :role :scd-metadata}]
        
        ;; Combine all columns
        all-columns (concat [surrogate-key] 
                           (when natural-key [natural-key])
                           attribute-columns
                           scd-columns)
        
        ;; Generate column definitions
        column-defs (mapv (fn [col]
                           (str "    " (:column-name col) " " (:data-type col)
                                (when (seq (:constraints col))
                                  (str " " (str/join " " (:constraints col))))))
                         all-columns)
        
        ;; Generate indexes
        indexes [(str "CREATE INDEX idx_" table-name "_natural_key ON " schema-name "." table-name 
                     " (" (:column-name natural-key) ");")
                (str "CREATE INDEX idx_" table-name "_current ON " schema-name "." table-name 
                     " (is_current) WHERE is_current = TRUE;")]
        
        ;; Main table DDL
        table-ddl (str "CREATE TABLE " schema-name "." table-name " (\n"
                      (str/join ",\n" column-defs)
                      "\n);")]
    
    {:table-ddl table-ddl
     :indexes indexes
     :table-name table-name
     :columns all-columns}))

(defn generate-snowflake-dimension-ddl
  "Generate DDL for normalized snowflake dimensions"
  [snowflake-dims schema-name config]
  (mapv (fn [dim-hierarchy]
          (let [parent-table (:parent-table dim-hierarchy)
                child-table (:child-table dim-hierarchy)]
            
            ;; Generate parent dimension table
            (let [parent-ddl (generate-dimension-table-ddl 
                             {:table-name (:name parent-table)
                              :natural-key {:property (:key-column parent-table)
                                          :role :identifier}
                              :attributes [{:property (:key-column parent-table)
                                          :role :dimension}]}
                             schema-name config)]
              
              ;; Generate child dimension table with FK to parent
              (let [child-ddl (generate-dimension-table-ddl
                              {:table-name (:name child-table)
                               :natural-key {:property (:key-column child-table)
                                           :role :identifier}
                               :attributes [{:property (:key-column child-table)
                                           :role :dimension}]}
                              schema-name config)
                    
                    ;; Add foreign key to parent
                    fk-constraint (str "ALTER TABLE " schema-name "." (:table-name child-ddl)
                                     " ADD CONSTRAINT fk_" (:table-name child-ddl) "_parent"
                                     " FOREIGN KEY (parent_dim_id)"
                                     " REFERENCES " schema-name "." (:table-name parent-ddl) "(dim_id);")]
                
                {:parent-table parent-ddl
                 :child-table child-ddl
                 :foreign-key-constraint fk-constraint}))))
        snowflake-dims))

(defn generate-schema-ddl
  "Generate complete schema DDL including schema creation"
  [schema-name]
  [(str "-- Create schema")
   (str "CREATE SCHEMA IF NOT EXISTS " schema-name ";")
   ""
   (str "-- Set search path")
   (str "SET search_path TO " schema-name ", public;")])

(defn generate-comments-ddl
  "Generate table and column comments for documentation"
  [table-ddl schema-name]
  (let [comments (atom [])]
    
    ;; Table comments
    (swap! comments conj
           (str "COMMENT ON TABLE " schema-name "." (:table-name table-ddl)
                " IS 'Auto-generated table from semantic analysis';"))
    
    ;; Column comments
    (doseq [col (:columns table-ddl)]
      (when (:semantic-type col)
        (swap! comments conj
               (str "COMMENT ON COLUMN " schema-name "." (:table-name table-ddl) "." (:column-name col)
                    " IS 'Semantic type: " (name (:semantic-type col)) "';")))) 
    
    @comments))

(defn generate-maintenance-functions
  "Generate maintenance functions for the schema"
  [schema-name]
  [(str "-- Maintenance function for SCD Type 2 updates")
   (str "CREATE OR REPLACE FUNCTION " schema-name ".update_dimension_scd()")
   "RETURNS TRIGGER AS $$"
   "BEGIN"
   "    -- Close current record"
   "    UPDATE " schema-name ".\" || TG_TABLE_NAME || \""
   "    SET expiry_date = CURRENT_DATE, is_current = FALSE"
   "    WHERE dim_id = NEW.dim_id AND is_current = TRUE;"
   "    "
   "    -- Insert new record"
   "    NEW.effective_date := CURRENT_DATE;"
   "    NEW.expiry_date := '9999-12-31';"
   "    NEW.is_current := TRUE;"
   "    "
   "    RETURN NEW;"
   "END;"
   "$$ LANGUAGE plpgsql;"])

(defn generate-ddl
  "Main function to generate complete PostgreSQL DDL"
  [schema-analysis table-name schema-name config]
  (log/info "Generating PostgreSQL DDL for table:" table-name "in schema:" schema-name)
  (log/info "Schema analysis keys:" (keys schema-analysis))
  
  (let [recommended-pattern (:recommended-pattern schema-analysis)
        pattern-type (:schema-pattern recommended-pattern)
        
        ;; Generate schema creation
        schema-ddl (generate-schema-ddl schema-name)
        
        ;; Generate table DDL based on pattern
        table-ddls (case pattern-type
                     :star
                     (let [fact-ddl (generate-fact-table-ddl 
                                   (:central-table recommended-pattern) 
                                   schema-name config)]
                       [fact-ddl])
                     
                     :snowflake
                     (let [fact-ddl (generate-fact-table-ddl 
                                   (:central-table recommended-pattern)
                                   schema-name config)
                           snowflake-ddls (generate-snowflake-dimension-ddl
                                         (:dimension-hierarchies recommended-pattern)
                                         schema-name config)]
                       (concat [fact-ddl] snowflake-ddls))
                     
                     :dimension-table
                     (let [table-structure (:table-structure recommended-pattern)
                           _ (log/info "Dimension table structure:" table-structure)
                           dim-ddl (if table-structure
                                    (generate-dimension-table-ddl table-structure schema-name config)
                                    {:table-ddl (str "CREATE TABLE " schema-name "." table-name " (\n"
                                                     "    dim_id BIGSERIAL PRIMARY KEY,\n"
                                                     "    -- Dimension table for " table-name "\n"
                                                     "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
                                                     ");")
                                     :table-name table-name
                                     :columns []})]
                       [dim-ddl])
                     
                     ;; Default: create table with actual columns from ontology analysis
                     (let [ontology-data (get-in schema-analysis [:ontology] {})
                           column-roles (:column-roles ontology-data)
                           _ (log/info "Default DDL: Found" (count column-roles) "columns to generate")
                           
                           ;; Generate column definitions from ontology analysis
                           column-defs (if (seq column-roles)
                                        (mapv #(generate-column-definition % :general config) column-roles)
                                        [])
                           
                           ;; Create DDL with all columns
                           all-columns (concat [{:column-name "id"
                                               :data-type "BIGSERIAL"
                                               :constraints ["PRIMARY KEY"]
                                               :role :surrogate-key}]
                                              column-defs
                                              [{:column-name "created_at"
                                               :data-type "TIMESTAMP"
                                               :constraints ["DEFAULT CURRENT_TIMESTAMP"]
                                               :role :metadata}])
                           
                           ;; Generate column DDL strings
                           column-ddl-strings (mapv (fn [col]
                                                     (str "    " (:column-name col) " " (:data-type col)
                                                          (when (seq (:constraints col))
                                                            (str " " (str/join " " (:constraints col))))))
                                                   all-columns)
                           
                           table-ddl (str "CREATE TABLE " schema-name "." table-name " (\n"
                                         (str/join ",\n" column-ddl-strings)
                                         "\n);")
                           
                           basic-ddl {:table-ddl table-ddl
                                     :table-name table-name
                                     :columns all-columns}]
                       [basic-ddl]))
        
        ;; Generate comments
        comment-ddls (mapcat #(generate-comments-ddl % schema-name) table-ddls)
        
        ;; Generate maintenance functions
        maintenance-ddls (generate-maintenance-functions schema-name)
        
        ;; Combine all DDL statements
        all-ddl (concat schema-ddl
                       (map :table-ddl table-ddls)
                       (mapcat :indexes table-ddls)
                       comment-ddls
                       maintenance-ddls)]
    
    (log/info "Generated" (count all-ddl) "DDL statements")
    (log/info "Schema pattern:" pattern-type)
    
    all-ddl))