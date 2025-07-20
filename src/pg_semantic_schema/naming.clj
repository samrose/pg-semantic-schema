(ns pg-semantic-schema.naming
  "Intelligent naming strategies for schemas and tables based on semantic analysis"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [pg-semantic-schema.ontology-mapping :as onto-map]))

(defn detect-domain-from-columns
  "Detect business domain from column names and semantic types using ontologies"
  [column-roles semantic-types]
  (let [domain-analysis (onto-map/analyze-domain-with-ontologies column-roles semantic-types)]
    (:detected-domain domain-analysis)))

(defn classify-table-purpose
  "Classify the purpose of the table based on its structure"
  [ontology-data]
  (let [column-roles (:column-roles ontology-data)
        table-type (:table-type ontology-data)
        measure-count (count (filter #(= (:role %) :measure) column-roles))
        dimension-count (count (filter #(contains? #{:dimension :categorical-dimension :temporal-dimension} 
                                                  (:role %)) column-roles))
        identifier-count (count (filter #(= (:role %) :identifier) column-roles))]
    
    (cond
      ;; Fact table indicators
      (and (> measure-count 1) (> dimension-count 2)) :fact
      (= table-type :fact-table) :fact
      
      ;; Dimension table indicators  
      (and (> dimension-count measure-count) (>= identifier-count 1)) :dimension
      (= table-type :dimension-table) :dimension
      
      ;; Bridge table (many identifiers, few measures)
      (and (> identifier-count 2) (< measure-count 2)) :bridge
      
      ;; Lookup table (high uniqueness, categorical data)
      (and (= identifier-count 1) (< measure-count 1)) :lookup
      
      ;; Transaction log (temporal + identifiers)
      (and (some #(= (:role %) :temporal-dimension) column-roles)
           (> identifier-count 1)) :transaction
      
      :else :general)))

(defn extract-key-concepts
  "Extract key business concepts from column names"
  [column-roles]
  (let [column-names (map #(-> (:property %) (str/split #"/") last str/lower-case) column-roles)
        
        ;; Extract meaningful words (filter out common technical terms)
        meaningful-words (reduce (fn [acc col-name]
                                  (let [words (-> col-name
                                                (str/replace #"_" " ")
                                                (str/replace #"(?i)id$|key$|date$|time$|created|updated|modified" "")
                                                (str/split #"\s+"))]
                                    (concat acc (filter #(> (count %) 2) words))))
                                [] column-names)
        
        ;; Count frequency and take most common
        word-counts (frequencies meaningful-words)
        top-concepts (take 3 (map first (sort-by second > word-counts)))]
    
    (filter #(not (contains? #{"and" "the" "for" "with" "from"} %)) top-concepts)))

(defn generate-schema-name
  "Generate an intelligent schema name based on analysis"
  [ontology-data semantic-types table-name]
  (let [domain (detect-domain-from-columns (:column-roles ontology-data) semantic-types)
        purpose (classify-table-purpose ontology-data)
        
        schema-name (case domain
                      :sales "sales_analytics"
                      :finance "financial_data"
                      :hr "human_resources"
                      :customer "customer_data"
                      :product "product_catalog"
                      :marketing "marketing_analytics"
                      :logistics "supply_chain"
                      :healthcare "patient_data"
                      :education "academic_data"
                      :retail "retail_operations"
                      :organization "organizational_data"
                      :general (str (str/replace table-name #"[^a-zA-Z0-9]" "_") "_schema"))]
    
    (log/info "Generated schema name:" schema-name "for domain:" domain)
    schema-name))

(defn generate-table-name
  "Generate an intelligent table name based on analysis"
  [ontology-data semantic-types original-table-name]
  (let [domain (detect-domain-from-columns (:column-roles ontology-data) semantic-types)
        purpose (classify-table-purpose ontology-data)
        key-concepts (extract-key-concepts (:column-roles ontology-data))
        
        ;; Base name from key concepts or original name
        base-name (if (seq key-concepts)
                   (str/join "_" (take 2 key-concepts))
                   (str/replace original-table-name #"[^a-zA-Z0-9]" "_"))
        
        ;; Add suffix based on purpose
        suffix (case purpose
                 :fact "_fact"
                 :dimension "_dim"
                 :bridge "_bridge"
                 :lookup "_lookup"
                 :transaction "_log"
                 :general "_data")
        
        ;; Combine and clean
        table-name (-> (str base-name suffix)
                      (str/replace #"_+" "_")
                      (str/replace #"^_|_$" "")
                      str/lower-case)]
    
    (log/info "Generated table name:" table-name 
              "for purpose:" purpose 
              "with concepts:" key-concepts)
    table-name))

(defn suggest-column-names
  "Suggest better column names based on semantic types"
  [column-roles semantic-types]
  (mapv (fn [col]
          (let [original-name (-> (:property col) (str/split #"/") last)
                semantic-type (:semantic-type col)
                role (:role col)
                
                ;; Suggest better name based on semantic type
                suggested-name (case (keyword semantic-type)
                                :email "email_address"
                                :phone "phone_number"
                                :currency "amount_usd"
                                :date "event_date"
                                :ssn "social_security_number"
                                :url "website_url"
                                :zip-code "postal_code"
                                original-name)
                
                ;; Add role-based suffix if needed
                final-name (case role
                             :identifier (if (str/ends-with? suggested-name "_id")
                                          suggested-name
                                          (str suggested-name "_id"))
                             :foreign-key (str suggested-name "_key")
                             suggested-name)]
            
            (assoc col :suggested-name final-name)))
        column-roles))

(defn generate-intelligent-names
  "Main function to generate all intelligent names"
  [ontology-data semantic-types original-table-name]
  (let [domain-analysis (onto-map/analyze-domain-with-ontologies (:column-roles ontology-data) semantic-types)
        schema-name (generate-schema-name ontology-data semantic-types original-table-name)
        table-name (generate-table-name ontology-data semantic-types original-table-name)
        suggested-columns (suggest-column-names (:column-roles ontology-data) semantic-types)]
    
    {:schema-name schema-name
     :table-name table-name
     :suggested-columns suggested-columns
     :detected-domain (:detected-domain domain-analysis)
     :table-purpose (classify-table-purpose ontology-data)
     :ontology-analysis domain-analysis}))