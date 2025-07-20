(ns pg-semantic-schema.ontology-discovery
  "Semantic pattern discovery using Jena reasoners and SPARQL"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.query QueryFactory QueryExecutionFactory]
           [org.apache.jena.vocabulary RDF RDFS OWL]
           [org.apache.jena.reasoner.transitiveReasoner TransitiveReasoner]
           [org.apache.jena.ontology OntModel]))

(defn create-inference-model
  "Create an inference model with reasoning capabilities"
  [base-model config]
  (let [reasoner (config/create-reasoner config)
        inf-model (ModelFactory/createInfModel reasoner base-model)]
    (log/info "Created inference model with" (.size inf-model) "inferred triples")
    inf-model))

(defn parse-rdf-literal
  "Parse RDF literal value, removing type annotations"
  [literal-str]
  (if (and literal-str (string? literal-str))
    (let [value (first (str/split literal-str #"\^\^"))]
      value)
    (str literal-str)))

(defn execute-sparql-query
  "Execute a SPARQL query and return results as maps"
  [model query-string]
  (let [query (QueryFactory/create query-string)
        qexec (QueryExecutionFactory/create query model)]
    (try
      (let [results (.execSelect qexec)
            result-vars (.getResultVars results)
            results-seq (iterator-seq results)]
        (mapv (fn [result]
                (reduce (fn [acc var]
                          (let [node (.get result var)]
                            (assoc acc (keyword var)
                                      (cond
                                        (.isLiteral node) (.toString node)
                                        (.isResource node) (.getURI node)
                                        :else (.toString node)))))
                        {} result-vars))
              results-seq))
      (finally
        (.close qexec)))))

(defn discover-semantic-types
  "Discover semantic types from the RDF model"
  [model config]
  (let [query "
    PREFIX base: <http://example.org/schema/data/>
    SELECT ?property ?semanticType ?confidence WHERE {
      ?property base:hasSemanticType ?semanticType .
      ?property base:semanticConfidence ?confidence .
    }"]
    
    (execute-sparql-query model query)))

(defn discover-functional-dependencies
  "Discover functional dependencies using co-occurrence analysis"
  [model config]
  (let [;; Find columns that have high correlation (potential functional dependencies)
        query "
        PREFIX base: <http://example.org/schema/data/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT ?prop1 ?prop2 (COUNT(*) as ?cooccurrence) WHERE {
          ?row ?prop1 ?val1 .
          ?row ?prop2 ?val2 .
          FILTER(?prop1 != ?prop2)
          FILTER(!CONTAINS(STR(?prop1), 'rowIndex'))
          FILTER(!CONTAINS(STR(?prop2), 'rowIndex'))
        }
        GROUP BY ?prop1 ?prop2
        ORDER BY DESC(?cooccurrence)"
        
        results (execute-sparql-query model query)]
    
    ;; Further analysis to determine actual functional dependencies
    (let [potential-fds (filter #(> (Integer/parseInt (parse-rdf-literal (:cooccurrence %))) 10) results)]
      (log/info "Found" (count potential-fds) "potential functional dependencies")
      potential-fds)))

(defn discover-hierarchies
  "Discover hierarchical relationships in the data"
  [model config]
  (let [query "
        PREFIX base: <http://example.org/schema/data/>
        
        SELECT ?relationship ?childColumn ?parentColumn ?coverage WHERE {
          ?relationship base:relationshipType 'hierarchical' .
          ?relationship base:childColumn ?childColumn .
          ?relationship base:parentColumn ?parentColumn .
          ?relationship base:coverage ?coverage .
        }
        ORDER BY DESC(?coverage)"]
    
    (execute-sparql-query model query)))

(defn discover-foreign-key-candidates
  "Discover potential foreign key relationships"
  [model config]
  (let [query "
        PREFIX base: <http://example.org/schema/data/>
        
        SELECT ?relationship ?fromColumn ?toColumn ?similarity WHERE {
          ?relationship base:relationshipType 'potential-foreign-key' .
          ?relationship base:fromColumn ?fromColumn .
          ?relationship base:toColumn ?toColumn .
          ?relationship base:similarity ?similarity .
        }
        ORDER BY DESC(?similarity)"]
    
    (execute-sparql-query model query)))

(defn analyze-cardinality
  "Analyze cardinality relationships between columns"
  [model config]
  (let [query "
        PREFIX base: <http://example.org/schema/data/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT ?property ?uniqueValues ?nullValues WHERE {
          ?property base:uniqueValues ?uniqueValues .
          ?property base:nullValues ?nullValues .
          ?property rdf:type rdf:Property .
        }"]
    
    (execute-sparql-query model query)))

(defn classify-column-roles
  "Classify columns into potential roles (dimension, measure, identifier, etc.)"
  [semantic-types cardinality-info]
  (mapv (fn [cardinality]
          (let [property (:property cardinality)
                unique-vals (Integer/parseInt (parse-rdf-literal (:uniqueValues cardinality)))
                null-vals (Integer/parseInt (parse-rdf-literal (:nullValues cardinality)))
                semantic-type (->> semantic-types
                                 (filter #(= (:property %) property))
                                 first
                                 :semanticType)
                
                ;; Classification logic
                role (cond
                       ;; High uniqueness suggests identifier or key
                       (> unique-vals 0.8) :identifier
                       
                       ;; Semantic types that suggest dimensions
                       (contains? #{:email :phone :url} (keyword semantic-type)) :dimension
                       
                       ;; Date/time columns are often dimensions
                       (contains? #{:date :time} (keyword semantic-type)) :temporal-dimension
                       
                       ;; Currency suggests measures
                       (= :currency (keyword semantic-type)) :measure
                       
                       ;; Low cardinality suggests categorical dimension
                       (< unique-vals 20) :categorical-dimension
                       
                       ;; Medium cardinality might be dimension
                       (< unique-vals 100) :dimension
                       
                       ;; High cardinality numeric might be measure
                       :else :measure)]
            
            (assoc cardinality
                   :semantic-type semantic-type
                   :role role
                   :uniqueness-ratio (/ unique-vals (+ unique-vals null-vals)))))
        cardinality-info))

(defn infer-table-type
  "Infer whether this represents a fact table or dimension table"
  [column-roles foreign-keys]
  (let [identifier-count (count (filter #(= (:role %) :identifier) column-roles))
        measure-count (count (filter #(= (:role %) :measure) column-roles))
        dimension-count (count (filter #(contains? #{:dimension :categorical-dimension :temporal-dimension} 
                                                  (:role %)) column-roles))
        fk-count (count foreign-keys)]
    
    (cond
      ;; Many measures and foreign keys suggest fact table
      (and (> measure-count 2) (> fk-count 1)) :fact-table
      
      ;; Many dimensions and few measures suggest dimension table
      (and (> dimension-count measure-count) (< fk-count 2)) :dimension-table
      
      ;; High identifier ratio suggests lookup/dimension table
      (> identifier-count (* 0.3 (count column-roles))) :dimension-table
      
      ;; Default classification
      :else :fact-table)))

(defn discover-business-rules
  "Discover potential business rules from the data patterns"
  [model config]
  (let [;; Find columns that are always non-null when another column has specific values
        constraint-query "
        PREFIX base: <http://example.org/schema/data/>
        
        SELECT ?prop1 ?prop2 WHERE {
          ?row ?prop1 ?val1 .
          ?row ?prop2 ?val2 .
          FILTER(?prop1 != ?prop2)
          FILTER(BOUND(?val1) && BOUND(?val2))
        }"
        
        results (execute-sparql-query model constraint-query)]
    
    ;; Analyze for business rules like "if email exists, then customer_type must be 'individual'"
    (log/info "Analyzing business rule patterns from" (count results) "property pairs")
    
    ;; This is a simplified version - in practice, you'd do more sophisticated pattern analysis
    {:constraint-patterns (take 10 results)
     :completeness-rules []
     :validation-rules []}))

(defn discover-ontology
  "Main function to discover ontological patterns from RDF model"
  [model config]
  (log/info "Starting ontology discovery")
  
  ;; Create inference model for reasoning
  (let [inf-model (create-inference-model model config)
        
        ;; Discover various patterns
        semantic-types (discover-semantic-types inf-model config)
        functional-deps (discover-functional-dependencies inf-model config)
        hierarchies (discover-hierarchies inf-model config)
        foreign-keys (discover-foreign-key-candidates inf-model config)
        cardinality-info (analyze-cardinality inf-model config)
        
        ;; Classify column roles
        column-roles (classify-column-roles semantic-types cardinality-info)
        
        ;; Infer table type
        table-type (infer-table-type column-roles foreign-keys)
        
        ;; Discover business rules
        business-rules (discover-business-rules inf-model config)]
    
    (log/info "Ontology discovery completed")
    (log/info "Found:" 
              (count semantic-types) "semantic types,"
              (count functional-deps) "functional dependencies,"
              (count hierarchies) "hierarchies,"
              (count foreign-keys) "foreign key candidates")
    (log/info "Inferred table type:" table-type)
    
    {:semantic-types semantic-types
     :functional-dependencies functional-deps
     :hierarchies hierarchies
     :foreign-key-candidates foreign-keys
     :cardinality-analysis cardinality-info
     :column-roles column-roles
     :table-type table-type
     :business-rules business-rules
     :inference-model inf-model}))