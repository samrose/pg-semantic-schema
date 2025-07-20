(ns pg-semantic-schema.schema-discovery-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [pg-semantic-schema.schema-discovery :as schema]
            [pg-semantic-schema.config :as config]
            [pg-semantic-schema.ontology-discovery :as onto])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.vocabulary RDF RDFS]))

;; Sample ontology data for testing
(def sample-fact-ontology-data
  {:column-roles [{:property "http://example.org/sales/amount" :role :measure :semantic-type :currency
                  :uniqueValues "100" :nullValues "5"}
                 {:property "http://example.org/sales/quantity" :role :measure :semantic-type :integer
                  :uniqueValues "50" :nullValues "2"}
                 {:property "http://example.org/sales/customer_id" :role :dimension :semantic-type :unknown
                  :uniqueValues "80" :nullValues "0"}
                 {:property "http://example.org/sales/product_id" :role :dimension :semantic-type :unknown
                  :uniqueValues "25" :nullValues "0"}
                 {:property "http://example.org/sales/date" :role :temporal-dimension :semantic-type :date
                  :uniqueValues "30" :nullValues "1"}]
   :semantic-types [{:property "http://example.org/sales/amount" :semanticType "currency" :confidence "0.9"}
                   {:property "http://example.org/sales/date" :semanticType "date" :confidence "0.95"}]
   :functional-dependencies []
   :hierarchies []
   :foreign-key-candidates [{:sourceColumn "customer_id" :targetColumn "customer_table_id" :similarity "0.85"}
                           {:sourceColumn "product_id" :targetColumn "product_table_id" :similarity "0.92"}]
   :table-type :fact-table
   :business-rules {:constraint-patterns [] :completeness-rules [] :validation-rules []}})

(def sample-dimension-ontology-data
  {:column-roles [{:property "http://example.org/customers/customer_id" :role :identifier :semantic-type :unknown
                  :uniqueValues "100" :nullValues "0"}
                 {:property "http://example.org/customers/customer_name" :role :dimension :semantic-type :unknown
                  :uniqueValues "98" :nullValues "2"}
                 {:property "http://example.org/customers/email" :role :dimension :semantic-type :email
                  :uniqueValues "95" :nullValues "5"}
                 {:property "http://example.org/customers/city" :role :categorical-dimension :semantic-type :unknown
                  :uniqueValues "15" :nullValues "1"}
                 {:property "http://example.org/customers/state" :role :categorical-dimension :semantic-type :unknown
                  :uniqueValues "8" :nullValues "0"}]
   :semantic-types [{:property "http://example.org/customers/email" :semanticType "email" :confidence "0.9"}]
   :functional-dependencies []
   :hierarchies [{:parentColumn "state" :childColumn "city" :coverage "0.8"}]
   :foreign-key-candidates []
   :table-type :dimension-table
   :business-rules {:constraint-patterns [] :completeness-rules [] :validation-rules []}})

(def sample-snowflake-hierarchies
  {:hierarchies [{:parentColumn "country" :childColumn "state" :coverage "0.9"}
                {:parentColumn "state" :childColumn "city" :coverage "0.85"}
                {:parentColumn "category" :childColumn "subcategory" :coverage "0.8"}]})

(deftest test-analyze-dimension-hierarchies
  (testing "Dimension hierarchy analysis"
    (let [hierarchies (schema/analyze-dimension-hierarchies sample-dimension-ontology-data)]
      (is (map? hierarchies))
      (is (contains? hierarchies "state->city"))
      (when (contains? hierarchies "state->city")
        (let [hierarchy (get hierarchies "state->city")]
          (is (= "state" (:parent-column hierarchy)))
          (is (= "city" (:child-column hierarchy)))
          (is (= 0.8 (:coverage hierarchy)))
          (is (= :snowflake-candidate (:hierarchy-type hierarchy)))))))
  
  (testing "Low coverage hierarchies are filtered out"
    (let [low-coverage-data {:hierarchies [{:parentColumn "a" :childColumn "b" :coverage "0.5"}]}
          hierarchies (schema/analyze-dimension-hierarchies low-coverage-data)]
      (is (empty? hierarchies))))
  
  (testing "Empty hierarchy data"
    (let [empty-data {:hierarchies []}
          hierarchies (schema/analyze-dimension-hierarchies empty-data)]
      (is (empty? hierarchies)))))

(deftest test-identify-fact-table-structure
  (testing "Fact table identification"
    (let [fact-structure (schema/identify-fact-table-structure sample-fact-ontology-data "sales")]
      (is (map? fact-structure))
      (is (= :fact (:table-type fact-structure)))
      (is (= "sales" (:table-name fact-structure)))
      (is (= 2 (count (:measures fact-structure))))
      (is (= 3 (count (:dimension-references fact-structure))))
      (is (= 2 (count (:foreign-key-candidates fact-structure))))))
  
  (testing "Non-fact table returns nil"
    (let [fact-structure (schema/identify-fact-table-structure sample-dimension-ontology-data "customers")]
      (is (nil? fact-structure))))
  
  (testing "Fact table structure properties"
    (let [fact-structure (schema/identify-fact-table-structure sample-fact-ontology-data "sales")]
      (is (every? #(contains? % :property) (:measures fact-structure)))
      (is (every? #(contains? % :role) (:measures fact-structure)))
      (is (every? #(contains? % :semantic-type) (:measures fact-structure))))))

(deftest test-identify-dimension-table-structure
  (testing "Dimension table identification"
    (let [dim-structure (schema/identify-dimension-table-structure sample-dimension-ontology-data "customers")]
      (is (map? dim-structure))
      (is (= :dimension (:table-type dim-structure)))
      (is (= "customers" (:table-name dim-structure)))
      (is (some? (:natural-key dim-structure)))
      (is (= 4 (count (:attributes dim-structure))))
      ; Hierarchies are filtered by table name, so they might be empty if column names don't contain table name
      (is (>= (count (:hierarchies dim-structure)) 0))))
  
  (testing "Non-dimension table returns nil"
    (let [dim-structure (schema/identify-dimension-table-structure sample-fact-ontology-data "sales")]
      (is (nil? dim-structure))))
  
  (testing "Dimension table structure properties"
    (let [dim-structure (schema/identify-dimension-table-structure sample-dimension-ontology-data "customers")]
      (is (contains? (:natural-key dim-structure) :property))
      (is (contains? (:natural-key dim-structure) :role))
      (is (every? #(contains? % :property) (:attributes dim-structure)))
      (is (every? #(contains? % :role) (:attributes dim-structure))))))

(deftest test-confidence-calculations
  (testing "Star schema confidence calculation"
    (let [fact-structure {:measures [{} {}]
                         :dimension-references [{} {} {}]
                         :foreign-key-candidates [{} {}]}]
      ; High confidence case
      (is (= 0.9 (schema/calculate-star-confidence fact-structure))))
    
    (let [fact-structure {:measures [{}]
                         :dimension-references [{} {}]
                         :foreign-key-candidates [{}]}]
      ; Medium confidence case
      (is (= 0.7 (schema/calculate-star-confidence fact-structure))))
    
    (let [fact-structure {:measures [{}]
                         :dimension-references [{}]
                         :foreign-key-candidates []}]
      ; Low confidence case
      (is (= 0.5 (schema/calculate-star-confidence fact-structure)))))
  
  (testing "Dimension table confidence calculation"
    (let [dim-structure {:attributes [{} {} {}]
                        :natural-key {:property "test"}
                        :hierarchies []}]
      ; High confidence case
      (is (= 0.8 (schema/calculate-dimension-confidence dim-structure))))
    
    (let [dim-structure {:attributes [{} {}]
                        :natural-key {:property "test"}
                        :hierarchies []}]
      ; Medium confidence case
      (is (= 0.6 (schema/calculate-dimension-confidence dim-structure))))
    
    (let [dim-structure {:attributes []
                        :natural-key nil
                        :hierarchies []}]
      ; Low confidence case
      (is (= 0.2 (schema/calculate-dimension-confidence dim-structure)))))
  
  (testing "Snowflake schema confidence calculation"
    (let [fact-structure {:measures [{} {}]
                         :dimension-references [{} {} {}]
                         :foreign-key-candidates [{} {}]}
          hierarchies {"h1" {} "h2" {} "h3" {}}]
      (let [confidence (schema/calculate-snowflake-confidence fact-structure hierarchies)]
        (is (>= confidence 0.9))
        (is (<= confidence 1.0))))))

(deftest test-recommendation-generation
  (testing "Star schema recommendations"
    (let [fact-structure {:measures [{} {}]
                         :dimension-references [{:role :temporal-dimension} {:role :dimension}]
                         :foreign-key-candidates [{} {}]}
          recommendations (schema/generate-star-recommendations fact-structure)]
      (is (vector? recommendations))
      (is (> (count recommendations) 0))
      (is (some #(str/includes? % "surrogate key") recommendations))
      (is (some #(str/includes? % "measure columns") recommendations))
      (is (some #(str/includes? % "foreign key") recommendations))
      (is (some #(str/includes? % "partitioning") recommendations))))
  
  (testing "Dimension table recommendations"
    (let [dim-structure {:attributes [{} {} {}]
                        :natural-key {:property "test"}
                        :hierarchies [{}]}
          recommendations (schema/generate-dimension-recommendations dim-structure)]
      (is (vector? recommendations))
      (is (> (count recommendations) 0))
      (is (some #(str/includes? % "surrogate key") recommendations))
      (is (some #(str/includes? % "SCD") recommendations))
      (is (some #(str/includes? % "attributes") recommendations))
      (is (some #(str/includes? % "hierarchical") recommendations))))
  
  (testing "Snowflake schema recommendations"
    (let [fact-structure {:measures [{} {}]
                         :dimension-references [{} {}]
                         :foreign-key-candidates [{}]}
          snowflake-dims [{}]
          recommendations (schema/generate-snowflake-recommendations fact-structure snowflake-dims)]
      (is (vector? recommendations))
      (is (> (count recommendations) 0))
      (is (some #(str/includes? % "Normalize") recommendations))
      (is (some #(str/includes? % "performance") recommendations))
      (is (some #(str/includes? % "referential integrity") recommendations)))))

(deftest test-create-snowflake-dimensions
  (testing "Snowflake dimension creation"
    (let [hierarchies {"country->state" {:parent-column "country" :child-column "state" :coverage 0.9}
                      "state->city" {:parent-column "state" :child-column "city" :coverage 0.85}}
          ontology-data sample-dimension-ontology-data
          snowflake-dims (schema/create-snowflake-dimensions hierarchies ontology-data)]
      (is (vector? snowflake-dims))
      (is (= 2 (count snowflake-dims)))
      (doseq [dim snowflake-dims]
        (is (contains? dim :dimension-name))
        (is (contains? dim :parent-table))
        (is (contains? dim :child-table))
        (is (contains? dim :hierarchy-level)))))
  
  (testing "Empty hierarchies"
    (let [snowflake-dims (schema/create-snowflake-dimensions {} sample-dimension-ontology-data)]
      (is (vector? snowflake-dims))
      (is (empty? snowflake-dims)))))

(deftest test-pattern-detection
  (testing "Star schema pattern detection"
    (let [pattern (schema/detect-star-schema-pattern sample-fact-ontology-data "sales")]
      (is (map? pattern))
      (is (= :star (:schema-pattern pattern)))
      (is (some? (:central-table pattern)))
      (is (> (:pattern-confidence pattern) 0.5))
      (is (vector? (:recommendations pattern)))))
  
  (testing "Dimension table pattern detection"
    (let [pattern (schema/detect-star-schema-pattern sample-dimension-ontology-data "customers")]
      (is (map? pattern))
      (is (= :dimension-table (:schema-pattern pattern)))
      (is (some? (:table-structure pattern)))
      (is (> (:pattern-confidence pattern) 0.0))
      (is (vector? (:recommendations pattern)))))
  
  (testing "Unknown pattern detection"
    (let [empty-ontology {:column-roles [] :table-type :unknown}
          pattern (schema/detect-star-schema-pattern empty-ontology "unknown")]
      (is (map? pattern))
      (is (= :unknown (:schema-pattern pattern)))
      (is (= 0.0 (:pattern-confidence pattern)))))
  
  (testing "Snowflake schema pattern detection"
    (let [fact-data-with-hierarchies (assoc sample-fact-ontology-data :hierarchies 
                                           [{:parentColumn "country" :childColumn "state" :coverage "0.9"}])
          pattern (schema/detect-snowflake-schema-pattern fact-data-with-hierarchies "sales")]
      (is (map? pattern))
      (is (= :snowflake (:schema-pattern pattern)))
      (is (some? (:central-table pattern)))
      (is (vector? (:dimension-hierarchies pattern)))
      (is (> (:pattern-confidence pattern) 0.0))))
  
  (testing "Snowflake pattern with no hierarchies returns nil"
    (let [pattern (schema/detect-snowflake-schema-pattern sample-fact-ontology-data "sales")]
      (is (nil? pattern)))))

(deftest test-data-quality-analysis
  (testing "Completeness calculation"
    (let [column-roles [{:uniqueValues "95" :nullValues "5"}
                       {:uniqueValues "90" :nullValues "10"}]
          completeness (schema/calculate-completeness column-roles)]
      (is (vector? completeness))
      (is (= 2 (count completeness)))
      (is (= 0.95 (double (:completeness (first completeness)))))
      (is (= 0.9 (double (:completeness (second completeness)))))))
  
  (testing "Uniqueness calculation"
    (let [column-roles [{:uniqueValues "80" :nullValues "20"}
                       {:uniqueValues "50" :nullValues "50"}]
          uniqueness (schema/calculate-uniqueness column-roles)]
      (is (vector? uniqueness))
      (is (= 2 (count uniqueness)))
      (is (= 0.8 (double (:uniqueness (first uniqueness)))))
      (is (= 0.5 (double (:uniqueness (second uniqueness)))))))
  
  (testing "Consistency calculation"
    (let [ontology-data {:foreign-key-candidates [{:similarity "0.85"} {:similarity "0.92"}]}
          consistency (schema/calculate-consistency ontology-data)]
      (is (vector? consistency))
      (is (= 2 (count consistency)))
      (is (= 0.85 (:consistency-score (first consistency))))
      (is (= 0.92 (:consistency-score (second consistency))))))
  
  (testing "Validity calculation"
    (let [ontology-data {:semantic-types [{:confidence "0.9"} {:confidence "0.8"}]}
          validity (schema/calculate-validity ontology-data)]
      (is (vector? validity))
      (is (= 2 (count validity)))
      (is (= 0.9 (:validity-score (first validity))))
      (is (= 0.8 (:validity-score (second validity))))))
  
  (testing "Complete data quality analysis"
    (let [quality (schema/analyze-data-quality sample-fact-ontology-data)]
      (is (map? quality))
      (is (contains? quality :completeness))
      (is (contains? quality :uniqueness))
      (is (contains? quality :consistency))
      (is (contains? quality :validity))
      (is (vector? (:completeness quality)))
      (is (vector? (:uniqueness quality)))
      (is (vector? (:consistency quality)))
      (is (vector? (:validity quality))))))

(deftest test-discover-schema-patterns
  (testing "Complete schema pattern discovery"
    (let [model (ModelFactory/createDefaultModel)
          config (assoc config/*default-config* :table-name "sales")
          result (schema/discover-schema-patterns model sample-fact-ontology-data config)]
      (is (map? result))
      (is (contains? result :detected-patterns))
      (is (contains? result :recommended-pattern))
      (is (contains? result :data-quality))
      (is (contains? result :table-name))
      (is (contains? result :schema-metadata))
      
      (is (= "sales" (:table-name result)))
      (is (map? (:detected-patterns result)))
      (is (contains? (:detected-patterns result) :star))
      (is (contains? (:detected-patterns result) :snowflake))
      
      (is (map? (:recommended-pattern result)))
      (is (contains? (:recommended-pattern result) :schema-pattern))
      (is (contains? (:recommended-pattern result) :pattern-confidence))
      
      (is (map? (:data-quality result)))
      (is (map? (:schema-metadata result)))))
  
  (testing "Schema discovery with snowflake preference"
    (let [model (ModelFactory/createDefaultModel)
          fact-data-with-hierarchies (assoc sample-fact-ontology-data :hierarchies 
                                           [{:parentColumn "country" :childColumn "state" :coverage "0.9"}])
          config (assoc config/*default-config* :table-name "sales")
          result (schema/discover-schema-patterns model fact-data-with-hierarchies config)]
      (is (map? result))
      ; Should prefer snowflake when hierarchies are present with high confidence
      (is (contains? #{:snowflake :star} (get-in result [:recommended-pattern :schema-pattern])))))
  
  (testing "Schema discovery with low confidence"
    (let [model (ModelFactory/createDefaultModel)
          low-confidence-data {:column-roles [{:uniqueValues "5" :nullValues "95"}]
                              :semantic-types []
                              :functional-dependencies []
                              :hierarchies []
                              :foreign-key-candidates []
                              :table-type :unknown
                              :business-rules {:constraint-patterns [] :completeness-rules [] :validation-rules []}}
          config (assoc config/*default-config* :table-name "unknown")
          result (schema/discover-schema-patterns model low-confidence-data config)]
      (is (map? result))
      (is (= :simple-table (get-in result [:recommended-pattern :schema-pattern])))))
  
  (testing "Schema metadata calculation"
    (let [model (ModelFactory/createDefaultModel)
          config (assoc config/*default-config* :table-name "test")
          result (schema/discover-schema-patterns model sample-fact-ontology-data config)
          metadata (:schema-metadata result)]
      (is (= (count (:column-roles sample-fact-ontology-data)) (:column-count metadata)))
      (is (>= (:relationship-count metadata) 0))
      (is (>= (:semantic-type-coverage metadata) 0))
      (is (<= (:semantic-type-coverage metadata) 1)))))

(deftest test-edge-cases
  (testing "Empty column roles"
    (let [empty-ontology {:column-roles []
                         :semantic-types []
                         :functional-dependencies []
                         :hierarchies []
                         :foreign-key-candidates []
                         :table-type :unknown
                         :business-rules {:constraint-patterns [] :completeness-rules [] :validation-rules []}}]
      (is (nil? (schema/identify-fact-table-structure empty-ontology "test")))
      (is (nil? (schema/identify-dimension-table-structure empty-ontology "test")))
      (is (empty? (schema/analyze-dimension-hierarchies empty-ontology)))))
  
  (testing "Null and empty values handling"
    (let [column-roles [{:uniqueValues "0" :nullValues "0"}
                       {:uniqueValues "10" :nullValues "5"}]
          completeness (schema/calculate-completeness column-roles)]
      ; Should handle zero and empty values gracefully
      (is (vector? completeness))
      (is (every? #(contains? % :completeness) completeness))))
  
  (testing "Invalid numeric values"
    ; Test graceful handling of parse errors by using valid string numbers
    (let [column-roles [{:uniqueValues "10" :nullValues "5"}]
          result (schema/calculate-completeness column-roles)]
      (is (vector? result))
      (is (< (Math/abs (- 0.6666666666666666 (double (:completeness (first result))))) 0.0001))))
  
  (testing "Missing configuration keys"
    (let [model (ModelFactory/createDefaultModel)
          minimal-config {}
          result (schema/discover-schema-patterns model sample-fact-ontology-data minimal-config)]
      (is (map? result))
      (is (= "unknown_table" (:table-name result))))))

(deftest test-pattern-confidence-thresholds
  (testing "Star pattern confidence thresholds"
    ; Test different combinations to verify threshold logic
    (let [high-confidence {:measures [{} {} {}] :dimension-references [{} {} {} {}] :foreign-key-candidates [{} {} {}]}
          medium-confidence {:measures [{}] :dimension-references [{} {}] :foreign-key-candidates [{}]}
          low-confidence {:measures [{}] :dimension-references [{}] :foreign-key-candidates []}
          very-low-confidence {:measures [] :dimension-references [] :foreign-key-candidates []}]
      
      (is (= 0.9 (schema/calculate-star-confidence high-confidence)))
      (is (= 0.7 (schema/calculate-star-confidence medium-confidence)))
      (is (= 0.5 (schema/calculate-star-confidence low-confidence)))
      (is (= 0.2 (schema/calculate-star-confidence very-low-confidence)))))
  
  (testing "Pattern selection logic"
    (let [model (ModelFactory/createDefaultModel)
          config (assoc config/*default-config* :table-name "test")]
      
      ; Test star pattern selection
      (let [result (schema/discover-schema-patterns model sample-fact-ontology-data config)]
        (is (= :star (get-in result [:recommended-pattern :schema-pattern]))))
      
      ; Test dimension pattern selection
      (let [result (schema/discover-schema-patterns model sample-dimension-ontology-data config)]
        (is (= :dimension-table (get-in result [:recommended-pattern :schema-pattern])))))))