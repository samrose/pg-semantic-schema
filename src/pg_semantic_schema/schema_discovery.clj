(ns pg-semantic-schema.schema-discovery
  "Star/snowflake schema pattern detection via SPARQL analysis"
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config]
            [pg-semantic-schema.ontology-discovery :as onto])
  (:import [java.util UUID]))

(defn analyze-dimension-hierarchies
  "Analyze potential dimension hierarchies for snowflake schema design"
  [ontology-data]
  (let [hierarchies (:hierarchies ontology-data)
        column-roles (:column-roles ontology-data)]
    
    ;; Group hierarchies by potential dimension chains
    (reduce (fn [acc hierarchy]
              (let [parent (:parentColumn hierarchy)
                    child (:childColumn hierarchy)
                    coverage (Double/parseDouble (:coverage hierarchy))]
                
                ;; Only consider high-coverage hierarchies for snowflake design
                (if (> coverage 0.7)
                  (let [chain-key (str parent "->" child)]
                    (assoc acc chain-key
                           {:parent-column parent
                            :child-column child
                            :coverage coverage
                            :hierarchy-type :snowflake-candidate}))
                  acc)))
            {} hierarchies)))

(defn identify-fact-table-structure
  "Identify the structure of a fact table based on ontology analysis"
  [ontology-data table-name]
  (let [column-roles (:column-roles ontology-data)
        foreign-keys (:foreign-key-candidates ontology-data)
        table-type (:table-type ontology-data)]
    
    (when (= table-type :fact-table)
      (let [measures (filter #(= (:role %) :measure) column-roles)
            dimensions (filter #(contains? #{:dimension :categorical-dimension :temporal-dimension} 
                                          (:role %)) column-roles)
            identifiers (filter #(= (:role %) :identifier) column-roles)]
        
        {:table-type :fact
         :table-name table-name
         :measures (mapv #(select-keys % [:property :role :semantic-type]) measures)
         :dimension-references (mapv #(select-keys % [:property :role :semantic-type]) dimensions)
         :natural-keys (mapv #(select-keys % [:property :role :semantic-type]) identifiers)
         :foreign-key-candidates foreign-keys}))))

(defn identify-dimension-table-structure
  "Identify the structure of dimension tables"
  [ontology-data table-name]
  (let [column-roles (:column-roles ontology-data)
        hierarchies (:hierarchies ontology-data)
        table-type (:table-type ontology-data)]
    
    (when (= table-type :dimension-table)
      (let [attributes (filter #(contains? #{:dimension :categorical-dimension :temporal-dimension} 
                                          (:role %)) column-roles)
            identifiers (filter #(= (:role %) :identifier) column-roles)
            measures (filter #(= (:role %) :measure) column-roles)]
        
        {:table-type :dimension
         :table-name table-name
         :natural-key (first identifiers)
         :attributes (mapv #(select-keys % [:property :role :semantic-type]) attributes)
         :measures (mapv #(select-keys % [:property :role :semantic-type]) measures)
         :hierarchies (filter #(or (str/includes? (:parentColumn %) table-name)
                                  (str/includes? (:childColumn %) table-name)) hierarchies)}))))

(defn calculate-star-confidence
  "Calculate confidence score for star schema classification"
  [fact-structure]
  (let [measure-count (count (:measures fact-structure))
        dimension-count (count (:dimension-references fact-structure))
        fk-count (count (:foreign-key-candidates fact-structure))]
    
    (cond
      (and (>= measure-count 2) (>= dimension-count 3) (>= fk-count 2)) 0.9
      (and (>= measure-count 1) (>= dimension-count 2) (>= fk-count 1)) 0.7
      (and (>= measure-count 1) (>= dimension-count 1)) 0.5
      :else 0.2)))

(defn calculate-dimension-confidence
  "Calculate confidence score for dimension table classification"
  [dim-structure]
  (let [attribute-count (count (:attributes dim-structure))
        has-natural-key (some? (:natural-key dim-structure))
        hierarchy-count (count (:hierarchies dim-structure))]
    
    (cond
      (and has-natural-key (>= attribute-count 3)) 0.8
      (and has-natural-key (>= attribute-count 2)) 0.6
      has-natural-key 0.4
      :else 0.2)))

(defn calculate-snowflake-confidence
  "Calculate confidence score for snowflake schema classification"
  [fact-structure hierarchies]
  (let [base-confidence (calculate-star-confidence fact-structure)
        hierarchy-bonus (min 0.2 (* 0.05 (count hierarchies)))]
    (min 1.0 (+ base-confidence hierarchy-bonus))))

(defn generate-star-recommendations
  "Generate recommendations for star schema implementation"
  [fact-structure]
  (let [recommendations (atom [])]
    
    ;; Surrogate key recommendation
    (swap! recommendations conj
           "Add surrogate key (auto-incrementing ID) as primary key")
    
    ;; Measure column recommendations
    (when (seq (:measures fact-structure))
      (swap! recommendations conj
             (str "Configure " (count (:measures fact-structure)) " measure columns with appropriate aggregation functions")))
    
    ;; Foreign key recommendations
    (when (seq (:foreign-key-candidates fact-structure))
      (swap! recommendations conj
             "Create foreign key relationships to dimension tables"))
    
    ;; Indexing recommendations
    (swap! recommendations conj
           "Create composite indexes on foreign key combinations for query performance")
    
    ;; Partitioning recommendations
    (let [temporal-dims (filter #(= (:role %) :temporal-dimension) (:dimension-references fact-structure))]
      (when (seq temporal-dims)
        (swap! recommendations conj
               "Consider date-based partitioning for improved query performance")))
    
    @recommendations))

(defn generate-dimension-recommendations
  "Generate recommendations for dimension table implementation"
  [dim-structure]
  (let [recommendations (atom [])]
    
    ;; Surrogate key recommendation
    (swap! recommendations conj
           "Add surrogate key as primary key, keep natural key as alternate key")
    
    ;; SCD recommendations
    (swap! recommendations conj
           "Consider Slowly Changing Dimension (SCD) Type 2 for historical tracking")
    
    ;; Attribute recommendations
    (when (seq (:attributes dim-structure))
      (swap! recommendations conj
             "Create descriptive attributes with proper data types and constraints"))
    
    ;; Hierarchy recommendations
    (when (seq (:hierarchies dim-structure))
      (swap! recommendations conj
             "Implement hierarchical relationships with bridge tables if needed"))
    
    @recommendations))

(defn generate-snowflake-recommendations
  "Generate recommendations for snowflake schema implementation"
  [fact-structure snowflake-dimensions]
  (let [star-recs (generate-star-recommendations fact-structure)
        snowflake-recs (atom star-recs)]
    
    ;; Normalization recommendations
    (swap! snowflake-recs conj
           "Normalize dimension hierarchies into separate tables to reduce redundancy")
    
    ;; Performance considerations
    (swap! snowflake-recs conj
           "Consider impact on query performance due to additional joins")
    
    ;; Maintenance recommendations
    (swap! snowflake-recs conj
           "Implement proper referential integrity between normalized dimension tables")
    
    @snowflake-recs))

(defn create-snowflake-dimensions
  "Create normalized dimension structures for snowflake schema"
  [hierarchies ontology-data]
  (mapv (fn [[chain-key hierarchy]]
          (let [parent-col (:parent-column hierarchy)
                child-col (:child-column hierarchy)]
            {:dimension-name (str "dim_" (str/replace parent-col #"[^a-zA-Z0-9]" "_"))
             :parent-table {:name (str "dim_" (str/replace parent-col #"[^a-zA-Z0-9]" "_"))
                           :key-column parent-col
                           :attributes [parent-col]}
             :child-table {:name (str "dim_" (str/replace child-col #"[^a-zA-Z0-9]" "_"))
                          :key-column child-col
                          :foreign-key parent-col
                          :attributes [child-col]}
             :hierarchy-level (:coverage hierarchy)}))
        hierarchies))

(defn detect-star-schema-pattern
  "Detect star schema patterns in the data"
  [ontology-data table-name]
  (let [fact-structure (identify-fact-table-structure ontology-data table-name)
        dim-structure (identify-dimension-table-structure ontology-data table-name)]
    
    (cond
      fact-structure
      {:schema-pattern :star
       :central-table fact-structure
       :pattern-confidence (calculate-star-confidence fact-structure)
       :recommendations (generate-star-recommendations fact-structure)}
      
      dim-structure
      {:schema-pattern :dimension-table
       :table-structure dim-structure
       :pattern-confidence (calculate-dimension-confidence dim-structure)
       :recommendations (generate-dimension-recommendations dim-structure)}
      
      :else
      {:schema-pattern :unknown
       :pattern-confidence 0.0
       :recommendations ["Unable to classify table structure"]})))

(defn detect-snowflake-schema-pattern
  "Detect snowflake schema patterns with normalized dimensions"
  [ontology-data table-name]
  (let [hierarchies (analyze-dimension-hierarchies ontology-data)
        fact-structure (identify-fact-table-structure ontology-data table-name)]
    
    (when (and fact-structure (seq hierarchies))
      (let [snowflake-dimensions (create-snowflake-dimensions hierarchies ontology-data)]
        {:schema-pattern :snowflake
         :central-table fact-structure
         :dimension-hierarchies snowflake-dimensions
         :pattern-confidence (calculate-snowflake-confidence fact-structure hierarchies)
         :recommendations (generate-snowflake-recommendations fact-structure snowflake-dimensions)}))))

(defn calculate-completeness
  "Calculate completeness scores for columns"
  [column-roles]
  (mapv (fn [col]
          (let [unique-vals (Integer/parseInt (onto/parse-rdf-literal (:uniqueValues col)))
                null-vals (Integer/parseInt (onto/parse-rdf-literal (:nullValues col)))
                total-vals (+ unique-vals null-vals)
                completeness (if (pos? total-vals)
                              (/ (- total-vals null-vals) total-vals)
                              0.0)]
            (assoc col :completeness completeness)))
        column-roles))

(defn calculate-uniqueness
  "Calculate uniqueness scores for potential key columns"
  [column-roles]
  (mapv (fn [col]
          (let [unique-vals (Integer/parseInt (onto/parse-rdf-literal (:uniqueValues col)))
                null-vals (Integer/parseInt (onto/parse-rdf-literal (:nullValues col)))
                total-vals (+ unique-vals null-vals)
                uniqueness (if (pos? total-vals)
                            (/ unique-vals total-vals)
                            0.0)]
            (assoc col :uniqueness uniqueness)))
        column-roles))

(defn calculate-consistency
  "Calculate consistency metrics across relationships"
  [ontology-data]
  (let [foreign-keys (:foreign-key-candidates ontology-data)]
    (mapv (fn [fk]
            (assoc fk :consistency-score (Double/parseDouble (onto/parse-rdf-literal (:similarity fk)))))
          foreign-keys)))

(defn calculate-validity
  "Calculate validity based on semantic type detection confidence"
  [ontology-data]
  (let [semantic-types (:semantic-types ontology-data)]
    (mapv (fn [st]
            (assoc st :validity-score (Double/parseDouble (onto/parse-rdf-literal (:confidence st)))))
          semantic-types)))

(defn analyze-data-quality
  "Analyze data quality aspects for schema design"
  [ontology-data]
  (let [column-roles (:column-roles ontology-data)]
    {:completeness (calculate-completeness column-roles)
     :uniqueness (calculate-uniqueness column-roles)
     :consistency (calculate-consistency ontology-data)
     :validity (calculate-validity ontology-data)}))

(defn discover-schema-patterns
  "Main function to discover star/snowflake schema patterns"
  [rdf-model ontology-data config]
  (log/info "Starting schema pattern discovery")
  
  (let [table-name (get-in config [:table-name] "unknown_table")
        
        ;; Detect different schema patterns
        star-pattern (detect-star-schema-pattern ontology-data table-name)
        snowflake-pattern (detect-snowflake-schema-pattern ontology-data table-name)
        
        ;; Analyze data quality
        quality-analysis (analyze-data-quality ontology-data)
        
        ;; Choose best pattern based on confidence scores
        best-pattern (cond
                       (and snowflake-pattern 
                            (> (:pattern-confidence snowflake-pattern) 
                               (:pattern-confidence star-pattern)))
                       snowflake-pattern
                       
                       (> (:pattern-confidence star-pattern) 0.5)
                       star-pattern
                       
                       :else
                       {:schema-pattern :simple-table
                        :pattern-confidence 0.3
                        :recommendations ["Consider manual schema design due to low pattern confidence"]})]
    
    (log/info "Schema pattern discovery completed")
    (log/info "Best pattern:" (:schema-pattern best-pattern) 
              "with confidence:" (:pattern-confidence best-pattern))
    
    {:detected-patterns {:star star-pattern
                        :snowflake snowflake-pattern}
     :recommended-pattern best-pattern
     :data-quality quality-analysis
     :table-name table-name
     :schema-metadata {:column-count (count (:column-roles ontology-data))
                      :relationship-count (+ (count (:foreign-key-candidates ontology-data))
                                           (count (:hierarchies ontology-data)))
                      :semantic-type-coverage (/ (count (filter :semantic-type (:semantic-types ontology-data)))
                                               (count (:semantic-types ontology-data)))}}))

(comment
  ;; Example usage for REPL exploration
  (require '[pg-semantic-schema.core :as core])
  (require '[pg-semantic-schema.rdf-conversion :as rdf])
  (require '[pg-semantic-schema.ontology-discovery :as onto])
  
  ;; Analyze a sample CSV
  (let [model (rdf/csv->rdf "sample.csv" config/*default-config*)
        ontology (onto/discover-ontology model config/*default-config*)
        schema (discover-schema-patterns model ontology config/*default-config*)]
    (clojure.pprint/pprint schema)))