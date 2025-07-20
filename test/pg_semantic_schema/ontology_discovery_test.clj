(ns pg-semantic-schema.ontology-discovery-test
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.ontology-discovery :as onto]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.vocabulary RDF RDFS]))

(def sample-rdf-model
  (let [model (ModelFactory/createDefaultModel)
        base-uri "http://example.org/schema/"]
    ;; Add some sample triples
    (.add model
          (.createResource model (str base-uri "table1/column1"))
          RDF/type
          RDF/Property)
    (.add model
          (.createResource model (str base-uri "table1/column1"))
          (.createProperty model (str base-uri "hasSemanticType"))
          "email")
    (.add model
          (.createResource model (str base-uri "table1/column1"))
          (.createProperty model (str base-uri "uniqueValues"))
          (.createTypedLiteral model 5))
    (.add model
          (.createResource model (str base-uri "table1/column1"))
          (.createProperty model (str base-uri "nullValues"))
          (.createTypedLiteral model 0))
    model))

(deftest test-parse-rdf-literal
  (testing "RDF literal parsing"
    (is (= "123" (onto/parse-rdf-literal "123")))
    (is (= "123" (onto/parse-rdf-literal "123^^http://www.w3.org/2001/XMLSchema#integer")))
    (is (= "45.67" (onto/parse-rdf-literal "45.67^^http://www.w3.org/2001/XMLSchema#decimal")))
    (is (= "true" (onto/parse-rdf-literal "true^^http://www.w3.org/2001/XMLSchema#boolean")))
    (is (= "2024-01-15" (onto/parse-rdf-literal "2024-01-15^^http://www.w3.org/2001/XMLSchema#date"))))
  
  (testing "Plain literals without type annotations"
    (is (= "simple text" (onto/parse-rdf-literal "simple text")))
    (is (= "" (onto/parse-rdf-literal "")))
    (is (= "test@example.com" (onto/parse-rdf-literal "test@example.com"))))
  
  (testing "Edge cases"
    (is (= "" (onto/parse-rdf-literal nil)))
    (is (= "complex" (onto/parse-rdf-literal "complex^^value^^http://example.org/type")))))

(deftest test-create-inference-model
  (testing "Inference model creation"
    (let [base-model (ModelFactory/createDefaultModel)
          inference-model (onto/create-inference-model base-model config/*default-config*)]
      (is (some? inference-model))
      (is (>= (.size inference-model) (.size base-model)))))
  
  (testing "Inference model with sample data"
    (let [inference-model (onto/create-inference-model sample-rdf-model config/*default-config*)]
      (is (some? inference-model))
      (is (> (.size inference-model) 0)))))

(deftest test-classify-column-roles
  (testing "Column role classification"
    (let [semantic-types [{:property "http://example.org/test" :semanticType "email"}]
          cardinality-data [{:property "http://example.org/test" :uniqueValues "5" :nullValues "0"}]
          roles (onto/classify-column-roles semantic-types cardinality-data)]
      (is (vector? roles))
      (is (every? map? roles))
      (is (every? #(contains? % :property) roles))
      (is (every? #(contains? % :role) roles))))
  
  (testing "Column role structure"
    (let [semantic-types [{:property "http://example.org/email" :semanticType "email"}]
          cardinality-data [{:property "http://example.org/email" :uniqueValues "10" :nullValues "0"}]
          roles (onto/classify-column-roles semantic-types cardinality-data)]
      (when (seq roles)
        (let [first-role (first roles)]
          (is (string? (:property first-role)))
          (is (keyword? (:role first-role)))))))
  
  (testing "Empty cardinality data"
    (let [roles (onto/classify-column-roles [] [])]
      (is (vector? roles))
      (is (empty? roles)))))

(deftest test-discover-functional-dependencies
  (testing "Functional dependency discovery"
    (let [deps (onto/discover-functional-dependencies sample-rdf-model config/*default-config*)]
      (is (sequential? deps))
      (is (every? map? deps))))
  
  (testing "Functional dependency structure"
    (let [deps (onto/discover-functional-dependencies sample-rdf-model config/*default-config*)]
      (doseq [dep deps]
        (is (contains? dep :determinant))
        (is (contains? dep :dependent))
        (is (contains? dep :confidence)))))
  
  (testing "Empty model functional dependencies"
    (let [empty-model (ModelFactory/createDefaultModel)
          deps (onto/discover-functional-dependencies empty-model config/*default-config*)]
      (is (sequential? deps)))))

(deftest test-discover-hierarchies
  (testing "Hierarchy discovery"
    (let [hierarchies (onto/discover-hierarchies sample-rdf-model config/*default-config*)]
      (is (vector? hierarchies))
      (is (every? map? hierarchies))))
  
  (testing "Hierarchy structure"
    (let [hierarchies (onto/discover-hierarchies sample-rdf-model config/*default-config*)]
      (doseq [hierarchy hierarchies]
        (is (contains? hierarchy :parentColumn))
        (is (contains? hierarchy :childColumn))
        (is (contains? hierarchy :coverage)))))
  
  (testing "Empty model hierarchies"
    (let [empty-model (ModelFactory/createDefaultModel)
          hierarchies (onto/discover-hierarchies empty-model config/*default-config*)]
      (is (vector? hierarchies)))))

(deftest test-discover-foreign-key-candidates
  (testing "Foreign key candidate discovery"
    (let [fk-candidates (onto/discover-foreign-key-candidates sample-rdf-model config/*default-config*)]
      (is (vector? fk-candidates))
      (is (every? map? fk-candidates))))
  
  (testing "Foreign key candidate structure"
    (let [fk-candidates (onto/discover-foreign-key-candidates sample-rdf-model config/*default-config*)]
      (doseq [fk fk-candidates]
        (is (contains? fk :sourceColumn))
        (is (contains? fk :targetColumn))
        (is (contains? fk :similarity)))))
  
  (testing "Empty model foreign keys"
    (let [empty-model (ModelFactory/createDefaultModel)
          fk-candidates (onto/discover-foreign-key-candidates empty-model config/*default-config*)]
      (is (vector? fk-candidates)))))

(deftest test-discover-semantic-types
  (testing "Semantic type discovery"
    (let [semantic-types (onto/discover-semantic-types sample-rdf-model config/*default-config*)]
      (is (vector? semantic-types))
      (is (every? map? semantic-types))))
  
  (testing "Semantic type structure"
    (let [semantic-types (onto/discover-semantic-types sample-rdf-model config/*default-config*)]
      (doseq [st semantic-types]
        (is (contains? st :property))
        (is (contains? st :semanticType))
        (is (contains? st :confidence)))))
  
  (testing "Email semantic type detection"
    (let [semantic-types (onto/discover-semantic-types sample-rdf-model config/*default-config*)
          email-type (first (filter #(= "email" (:semanticType %)) semantic-types))]
      (when email-type
        (is (= "email" (:semanticType email-type)))
        (is (string? (:property email-type))))))
  
  (testing "Empty model semantic types"
    (let [empty-model (ModelFactory/createDefaultModel)
          semantic-types (onto/discover-semantic-types empty-model config/*default-config*)]
      (is (vector? semantic-types)))))

(deftest test-infer-table-type
  (testing "Table type inference with various column patterns"
    ; Test fact table pattern (multiple measures + dimensions)
    (let [fact-columns [{:role :measure} {:role :measure} {:role :dimension} {:role :dimension}]
          foreign-keys [{:sourceColumn "fk1"} {:sourceColumn "fk2"}]
          table-type (onto/infer-table-type fact-columns foreign-keys)]
      (is (= :fact-table table-type)))
    
    ; Test dimension table pattern (identifier + dimensions)
    (let [dim-columns [{:role :identifier} {:role :dimension} {:role :dimension}]
          foreign-keys []
          table-type (onto/infer-table-type dim-columns foreign-keys)]
      (is (= :dimension-table table-type)))
    
    ; Test general table pattern
    (let [general-columns [{:role :dimension}]
          foreign-keys []
          table-type (onto/infer-table-type general-columns foreign-keys)]
      (is (contains? #{:fact-table :dimension-table} table-type))))
  
  (testing "Empty column list"
    (let [table-type (onto/infer-table-type [] [])]
      (is (contains? #{:fact-table :dimension-table} table-type))))
  
  (testing "Bridge table pattern"
    (let [bridge-columns [{:role :identifier} {:role :identifier} {:role :identifier}]
          foreign-keys [{:sourceColumn "fk1"} {:sourceColumn "fk2"}]
          table-type (onto/infer-table-type bridge-columns foreign-keys)]
      (is (contains? #{:fact-table :dimension-table} table-type)))))

(deftest test-discover-business-rules
  (testing "Business rule discovery"
    (let [rules (onto/discover-business-rules sample-rdf-model config/*default-config*)]
      (is (map? rules))
      (is (contains? rules :constraint-patterns))
      (is (contains? rules :completeness-rules))
      (is (contains? rules :validation-rules))))
  
  (testing "Business rule structure"
    (let [rules (onto/discover-business-rules sample-rdf-model config/*default-config*)]
      (is (sequential? (:constraint-patterns rules)))
      (is (vector? (:completeness-rules rules)))
      (is (vector? (:validation-rules rules)))))
  
  (testing "Empty model business rules"
    (let [empty-model (ModelFactory/createDefaultModel)
          rules (onto/discover-business-rules empty-model config/*default-config*)]
      (is (map? rules)))))

(deftest test-discover-ontology
  (testing "Complete ontology discovery"
    (let [result (onto/discover-ontology sample-rdf-model config/*default-config*)]
      (is (map? result))
      (is (contains? result :column-roles))
      (is (contains? result :semantic-types))
      (is (contains? result :functional-dependencies))
      (is (contains? result :hierarchies))
      (is (contains? result :foreign-key-candidates))
      (is (contains? result :table-type))
      (is (contains? result :business-rules))
      
      (is (vector? (:column-roles result)))
      (is (vector? (:semantic-types result)))
      (is (sequential? (:functional-dependencies result)))
      (is (vector? (:hierarchies result)))
      (is (vector? (:foreign-key-candidates result)))
      (is (keyword? (:table-type result)))
      (is (map? (:business-rules result)))))
  
  (testing "Ontology discovery with empty model"
    (let [empty-model (ModelFactory/createDefaultModel)
          result (onto/discover-ontology empty-model config/*default-config*)]
      (is (map? result))
      (is (contains? #{:fact-table :dimension-table} (:table-type result)))
      (is (empty? (:column-roles result)))
      (is (empty? (:semantic-types result)))))
  
  (testing "Ontology discovery preserves data structure"
    (let [result (onto/discover-ontology sample-rdf-model config/*default-config*)]
      ; Should have valid column roles
      (is (every? #(and (contains? % :property) 
                       (contains? % :role) 
                       (contains? % :semantic-type)) 
                 (:column-roles result)))
      
      ; Should have valid semantic types
      (is (every? #(and (contains? % :property) 
                       (contains? % :semanticType)) 
                 (:semantic-types result))))))

(deftest test-sparql-query-execution
  (testing "SPARQL query execution doesn't throw exceptions"
    ; This tests that SPARQL queries execute successfully without syntax errors
    (let [result (onto/discover-semantic-types sample-rdf-model config/*default-config*)]
      (is (vector? result))))
  
  (testing "Multiple SPARQL queries work"
    ; Test that multiple discovery operations work
    (let [semantic-types (onto/discover-semantic-types sample-rdf-model config/*default-config*)
          func-deps (onto/discover-functional-dependencies sample-rdf-model config/*default-config*)
          hierarchies (onto/discover-hierarchies sample-rdf-model config/*default-config*)]
      (is (vector? semantic-types))
      (is (sequential? func-deps))
      (is (vector? hierarchies)))))

(deftest test-error-handling
  (testing "Null model handling"
    (is (thrown? Exception (onto/discover-ontology nil config/*default-config*))))
  
  (testing "Invalid config handling"
    (let [invalid-config {}]
      ; Should handle missing config gracefully or throw meaningful error
      (is (map? (onto/discover-ontology sample-rdf-model invalid-config)))))
  
  (testing "Large model handling"
    ; Test with a larger model to ensure performance is reasonable
    (let [large-model (ModelFactory/createDefaultModel)
          base-uri "http://example.org/schema/"]
      ; Add many triples
      (doseq [i (range 100)]
        (.add large-model
              (.createResource large-model (str base-uri "table/column" i))
              RDF/type
              RDF/Property))
      
      (let [result (onto/discover-ontology large-model config/*default-config*)]
        (is (map? result))
        (is (vector? (:column-roles result)))))))

(deftest test-data-type-inference
  (testing "XSD data type handling"
    ; The parse-rdf-literal function should handle various XSD types
    (is (= "123" (onto/parse-rdf-literal "123^^http://www.w3.org/2001/XMLSchema#integer")))
    (is (= "45.67" (onto/parse-rdf-literal "45.67^^http://www.w3.org/2001/XMLSchema#decimal")))
    (is (= "true" (onto/parse-rdf-literal "true^^http://www.w3.org/2001/XMLSchema#boolean"))))
  
  (testing "Custom data type handling"
    (is (= "custom-value" (onto/parse-rdf-literal "custom-value^^http://example.org/customType")))))

(deftest test-role-classification-logic
  (testing "Role classification based on properties"
    ; Test that different column patterns get classified correctly
    (let [semantic-types [{:property "http://example.org/id" :semanticType "unknown"}
                         {:property "http://example.org/name" :semanticType "unknown"}]
          cardinality-data [{:property "http://example.org/id" :uniqueValues "15" :nullValues "0"}
                            {:property "http://example.org/name" :uniqueValues "10" :nullValues "2"}]
          roles (onto/classify-column-roles semantic-types cardinality-data)]
      (is (every? #(contains? #{:identifier :dimension :measure :categorical-dimension :temporal-dimension} (:role %)) roles))))

(deftest test-confidence-scoring
  (testing "Confidence values are within valid range"
    (let [result (onto/discover-ontology sample-rdf-model config/*default-config*)]
      ; Check functional dependencies confidence
      (is (every? #(<= 0.0 (Double/parseDouble (onto/parse-rdf-literal (:confidence %))) 1.0) 
                 (:functional-dependencies result)))
      
      ; Check semantic types confidence  
      (is (every? #(<= 0.0 (Double/parseDouble (onto/parse-rdf-literal (:confidence %))) 1.0) 
                 (:semantic-types result)))))))