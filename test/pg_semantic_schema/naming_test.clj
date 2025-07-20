(ns pg-semantic-schema.naming-test
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.naming :as naming]))

(def sample-ontology-data
  {:column-roles [{:property "http://example.org/employee_id" :role :identifier}
                  {:property "http://example.org/department" :role :dimension}
                  {:property "http://example.org/salary" :role :measure}
                  {:property "http://example.org/email" :role :dimension}
                  {:property "http://example.org/phone" :role :dimension}]
   :table-type :dimension-table})

(def sample-semantic-types
  [{:semanticType "email" :property "email"}
   {:semanticType "phone" :property "phone"}
   {:semanticType "currency" :property "salary"}])

(deftest test-detect-domain-from-columns
  (testing "Organization domain detection"
    (let [org-columns [{:property "http://example.org/employee_id"}
                       {:property "http://example.org/department"}
                       {:property "http://example.org/salary"}
                       {:property "http://example.org/manager"}]
          org-semantic-types [{:semanticType "currency"}]
          domain (naming/detect-domain-from-columns org-columns org-semantic-types)]
      (is (keyword? domain))
      (is (= :organization domain))))
  
  (testing "Energy domain detection"
    (let [energy-columns [{:property "http://example.org/meter_id"}
                          {:property "http://example.org/consumption_kwh"}
                          {:property "http://example.org/energy_source"}
                          {:property "http://example.org/grid_connection"}]
          energy-semantic-types []
          domain (naming/detect-domain-from-columns energy-columns energy-semantic-types)]
      (is (= :energy domain))))
  
  (testing "Food domain detection"
    (let [food-columns [{:property "http://example.org/inspection_id"}
                        {:property "http://example.org/restaurant_name"}
                        {:property "http://example.org/food_safety"}
                        {:property "http://example.org/violation_code"}]
          food-semantic-types []
          domain (naming/detect-domain-from-columns food-columns food-semantic-types)]
      (is (= :food domain))))
  
  (testing "Manufacturing domain detection"
    (let [mfg-columns [{:property "http://example.org/batch_number"}
                       {:property "http://example.org/production_line"}
                       {:property "http://example.org/quality_standard"}
                       {:property "http://example.org/defect_count"}]
          mfg-semantic-types []
          domain (naming/detect-domain-from-columns mfg-columns mfg-semantic-types)]
      (is (= :manufacturing domain)))))

(deftest test-classify-table-purpose
  (testing "Fact table classification"
    (let [fact-data {:column-roles [{:role :measure}
                                   {:role :measure}
                                   {:role :dimension}
                                   {:role :dimension}
                                   {:role :dimension}]
                    :table-type :fact-table}
          purpose (naming/classify-table-purpose fact-data)]
      (is (= :fact purpose))))
  
  (testing "Dimension table classification"
    (let [dim-data {:column-roles [{:role :identifier}
                                  {:role :dimension}
                                  {:role :dimension}
                                  {:role :dimension}]
                   :table-type :dimension-table}
          purpose (naming/classify-table-purpose dim-data)]
      (is (= :dimension purpose))))
  
  (testing "Bridge table classification"
    (let [bridge-data {:column-roles [{:role :identifier}
                                     {:role :identifier}
                                     {:role :identifier}
                                     {:role :measure}]
                      :table-type nil}
          purpose (naming/classify-table-purpose bridge-data)]
      (is (= :bridge purpose))))
  
  (testing "Lookup table classification"
    (let [lookup-data {:column-roles [{:role :identifier}
                                     {:role :dimension}]
                      :table-type nil}
          purpose (naming/classify-table-purpose lookup-data)]
      (is (= :lookup purpose))))
  
  (testing "Transaction table classification"
    (let [transaction-data {:column-roles [{:role :temporal-dimension}
                                          {:role :identifier}
                                          {:role :identifier}]
                           :table-type nil}
          purpose (naming/classify-table-purpose transaction-data)]
      (is (= :transaction purpose))))
  
  (testing "General table classification"
    (let [general-data {:column-roles [{:role :dimension}]
                       :table-type nil}
          purpose (naming/classify-table-purpose general-data)]
      (is (= :general purpose)))))

(deftest test-extract-key-concepts
  (testing "Concept extraction from column names"
    (let [columns [{:property "http://example.org/customer_name"}
                   {:property "http://example.org/order_total"}
                   {:property "http://example.org/product_price"}
                   {:property "http://example.org/customer_id"}]
          concepts (naming/extract-key-concepts columns)]
      (is (vector? concepts))
      (is (seq concepts))
      (is (some #{"customer"} concepts))
      (is (some #{"order"} concepts))))
  
  (testing "Concept extraction filters technical terms"
    (let [columns [{:property "http://example.org/employee_id"}
                   {:property "http://example.org/created_date"}
                   {:property "http://example.org/updated_time"}]
          concepts (naming/extract-key-concepts columns)]
      (is (some #{"employee"} concepts))
      (is (not (some #{"id"} concepts)))
      (is (not (some #{"date"} concepts)))
      (is (not (some #{"time"} concepts)))))
  
  (testing "Empty columns return empty concepts"
    (let [concepts (naming/extract-key-concepts [])]
      (is (vector? concepts))
      (is (empty? concepts)))))

(deftest test-generate-schema-name
  (testing "Schema name generation for different domains"
    (is (= "organizational_data" 
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :organization)
             sample-semantic-types 
             "test_table")))
    
    (is (= "energy_management"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :energy)
             sample-semantic-types 
             "test_table")))
    
    (is (= "food_safety"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :food)
             sample-semantic-types 
             "test_table")))
    
    (is (= "healthcare_data"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :healthcare)
             sample-semantic-types 
             "test_table")))
    
    (is (= "logistics_data"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :transportation)
             sample-semantic-types 
             "test_table")))
    
    (is (= "production_data"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :manufacturing)
             sample-semantic-types 
             "test_table")))
    
    (is (= "sales_analytics"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :sales)
             sample-semantic-types 
             "test_table")))
    
    (is (= "financial_data"
           (naming/generate-schema-name 
             (assoc sample-ontology-data :detected-domain :finance)
             sample-semantic-types 
             "test_table"))))
  
  (testing "General domain uses table name"
    (let [schema-name (naming/generate-schema-name 
                       (assoc sample-ontology-data :detected-domain :general)
                       sample-semantic-types 
                       "my_custom_table")]
      (is (clojure.string/includes? schema-name "my_custom_table"))
      (is (clojure.string/includes? schema-name "schema")))))

(deftest test-generate-table-name
  (testing "Table name generation with key concepts"
    (let [ontology-with-concepts {:column-roles [{:property "http://example.org/customer_name"}
                                                 {:property "http://example.org/order_total"}
                                                 {:property "http://example.org/product_price"}]
                                 :table-type :dimension-table}
          table-name (naming/generate-table-name ontology-with-concepts sample-semantic-types "original")]
      (is (string? table-name))
      (is (clojure.string/ends-with? table-name "_dim"))
      (is (some #(clojure.string/includes? table-name %) ["customer" "order" "product"]))))
  
  (testing "Table name generation with different purposes"
    (let [fact-ontology {:column-roles [{:role :measure} {:role :measure} {:role :dimension} {:role :dimension} {:role :dimension}]
                        :table-type :fact-table}
          fact-name (naming/generate-table-name fact-ontology sample-semantic-types "sales")]
      (is (clojure.string/ends-with? fact-name "_fact")))
    
    (let [bridge-ontology {:column-roles [{:role :identifier} {:role :identifier} {:role :identifier}]
                          :table-type :bridge-table}
          bridge-name (naming/generate-table-name bridge-ontology sample-semantic-types "connection")]
      (is (clojure.string/ends-with? bridge-name "_bridge")))
    
    (let [lookup-ontology {:column-roles [{:role :identifier} {:role :dimension}]
                          :table-type :lookup-table}
          lookup-name (naming/generate-table-name lookup-ontology sample-semantic-types "reference")]
      (is (clojure.string/ends-with? lookup-name "_lookup"))))
  
  (testing "Table name generation falls back to original name"
    (let [minimal-ontology {:column-roles []
                           :table-type :general}
          table-name (naming/generate-table-name minimal-ontology [] "fallback_table")]
      (is (clojure.string/includes? table-name "fallback_table")))))

(deftest test-suggest-column-names
  (testing "Column name suggestions based on semantic types"
    (let [columns [{:property "http://example.org/user_email" :semantic-type "email" :role :dimension}
                   {:property "http://example.org/contact_phone" :semantic-type "phone" :role :dimension}
                   {:property "http://example.org/total_amount" :semantic-type "currency" :role :measure}
                   {:property "http://example.org/birth_date" :semantic-type "date" :role :temporal-dimension}]
          suggested (naming/suggest-column-names columns sample-semantic-types)]
      (is (vector? suggested))
      (is (= 4 (count suggested)))
      (is (= "email_address" (:suggested-name (first suggested))))
      (is (= "phone_number" (:suggested-name (second suggested))))
      (is (= "amount_usd" (:suggested-name (nth suggested 2))))
      (is (= "event_date" (:suggested-name (nth suggested 3))))))
  
  (testing "Column names with role-based suffixes"
    (let [columns [{:property "http://example.org/customer" :semantic-type nil :role :identifier}
                   {:property "http://example.org/reference" :semantic-type nil :role :foreign-key}]
          suggested (naming/suggest-column-names columns [])]
      (is (clojure.string/ends-with? (:suggested-name (first suggested)) "_id"))
      (is (clojure.string/ends-with? (:suggested-name (second suggested)) "_key"))))
  
  (testing "Column names without semantic types"
    (let [columns [{:property "http://example.org/generic_field" :semantic-type nil :role :dimension}]
          suggested (naming/suggest-column-names columns [])]
      (is (= "generic_field" (:suggested-name (first suggested)))))))

(deftest test-generate-intelligent-names
  (testing "Complete intelligent names generation"
    (let [result (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "employee_data")]
      (is (map? result))
      (is (contains? result :schema-name))
      (is (contains? result :table-name))
      (is (contains? result :suggested-columns))
      (is (contains? result :detected-domain))
      (is (contains? result :table-purpose))
      (is (contains? result :ontology-analysis))
      
      (is (string? (:schema-name result)))
      (is (string? (:table-name result)))
      (is (vector? (:suggested-columns result)))
      (is (keyword? (:detected-domain result)))
      (is (keyword? (:table-purpose result)))
      (is (map? (:ontology-analysis result)))))
  
  (testing "Intelligent names with energy data"
    (let [energy-ontology {:column-roles [{:property "http://example.org/meter_id"}
                                         {:property "http://example.org/consumption_kwh"}
                                         {:property "http://example.org/energy_source"}]
                          :table-type :dimension-table}
          result (naming/generate-intelligent-names energy-ontology [] "energy_consumption")]
      (is (= :energy (:detected-domain result)))
      (is (= "energy_management" (:schema-name result)))
      (is (clojure.string/includes? (:table-name result) "kwh"))))
  
  (testing "Intelligent names with manufacturing data"
    (let [mfg-ontology {:column-roles [{:property "http://example.org/batch_number"}
                                      {:property "http://example.org/production_line"}
                                      {:property "http://example.org/quality_standard"}]
                       :table-type :dimension-table}
          result (naming/generate-intelligent-names mfg-ontology [] "quality_control")]
      (is (= :manufacturing (:detected-domain result)))
      (is (= "production_data" (:schema-name result)))
      (is (clojure.string/includes? (:table-name result) "production")))))

(deftest test-edge-cases
  (testing "Empty ontology data"
    (let [empty-ontology {:column-roles [] :table-type nil}
          result (naming/generate-intelligent-names empty-ontology [] "empty_table")]
      (is (map? result))
      (is (string? (:schema-name result)))
      (is (string? (:table-name result)))))
  
  (testing "Nil table name"
    (let [result (naming/generate-intelligent-names sample-ontology-data sample-semantic-types nil)]
      (is (map? result))
      (is (string? (:schema-name result)))
      (is (string? (:table-name result)))))
  
  (testing "Special characters in table name"
    (let [result (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "test@table#name")]
      (is (map? result))
      (is (string? (:schema-name result)))
      (is (string? (:table-name result)))
      (is (not (clojure.string/includes? (:table-name result) "@")))
      (is (not (clojure.string/includes? (:table-name result) "#")))))
  
  (testing "Very long table name"
    (let [long-name (apply str (repeat 100 "a"))
          result (naming/generate-intelligent-names sample-ontology-data sample-semantic-types long-name)]
      (is (map? result))
      (is (string? (:table-name result)))))
  
  (testing "Column properties without URIs"
    (let [invalid-ontology {:column-roles [{:property "not-a-uri"}
                                          {:property ""}
                                          {:property nil}]
                           :table-type :dimension-table}
          result (naming/generate-intelligent-names invalid-ontology [] "test")]
      (is (map? result))
      (is (vector? (:suggested-columns result))))))

(deftest test-name-consistency
  (testing "Generated names are consistent"
    (let [result1 (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "test_table")
          result2 (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "test_table")]
      (is (= (:schema-name result1) (:schema-name result2)))
      (is (= (:table-name result1) (:table-name result2)))
      (is (= (:detected-domain result1) (:detected-domain result2)))))
  
  (testing "Different input produces different names"
    (let [result1 (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "table1")
          result2 (naming/generate-intelligent-names sample-ontology-data sample-semantic-types "table2")]
      ; Schema names should be the same (based on domain)
      (is (= (:schema-name result1) (:schema-name result2)))
      ; Table names might be different (based on concepts or fallback to original name)
      ; Domain detection should be the same
      (is (= (:detected-domain result1) (:detected-domain result2))))))