(ns pg-semantic-schema.ontology-mapping-test
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.ontology-mapping :as onto-map]))

(def sample-column-roles
  [{:property "http://example.org/employee_id" :role :identifier}
   {:property "http://example.org/department" :role :dimension}
   {:property "http://example.org/salary" :role :measure}
   {:property "http://example.org/email" :role :dimension}
   {:property "http://example.org/phone" :role :dimension}])

(def sample-semantic-types
  [{:semanticType :email :property "email"}
   {:semanticType :phone :property "phone"}
   {:semanticType :currency :property "salary"}])

(deftest test-vocabularies
  (testing "Standard vocabularies are defined"
    (is (map? onto-map/vocabularies))
    (is (contains? onto-map/vocabularies :schema-org))
    (is (contains? onto-map/vocabularies :good-relations))
    (is (contains? onto-map/vocabularies :foaf))
    (is (string? (:schema-org onto-map/vocabularies)))
    (is (.startsWith (:schema-org onto-map/vocabularies) "http"))))

(deftest test-schema-org-mappings
  (testing "Schema.org mappings structure"
    (is (map? onto-map/schema-org-mappings))
    (is (contains? onto-map/schema-org-mappings :sales))
    (is (contains? onto-map/schema-org-mappings :finance))
    (is (contains? onto-map/schema-org-mappings :customer))
    (is (contains? onto-map/schema-org-mappings :organization))
    (is (contains? onto-map/schema-org-mappings :energy))
    (is (contains? onto-map/schema-org-mappings :food))
    (is (contains? onto-map/schema-org-mappings :healthcare))
    (is (contains? onto-map/schema-org-mappings :transportation))
    (is (contains? onto-map/schema-org-mappings :manufacturing)))
  
  (testing "Each domain has concepts and properties"
    (doseq [[domain mapping] onto-map/schema-org-mappings]
      (is (contains? mapping :concepts) (str "Domain " domain " missing :concepts"))
      (is (contains? mapping :properties) (str "Domain " domain " missing :properties"))
      (is (vector? (:concepts mapping)) (str "Domain " domain " :concepts not a vector"))
      (is (vector? (:properties mapping)) (str "Domain " domain " :properties not a vector"))
      (is (seq (:concepts mapping)) (str "Domain " domain " has empty :concepts"))
      (is (seq (:properties mapping)) (str "Domain " domain " has empty :properties")))))

(deftest test-normalize-term
  (testing "Term normalization"
    (is (= "employee" (onto-map/normalize-term "employee_id")))
    (is (= "customer" (onto-map/normalize-term "customer_key")))
    (is (= "order" (onto-map/normalize-term "order_date")))
    (is (= "sales" (onto-map/normalize-term "sales_time")))
    (is (= "total" (onto-map/normalize-term "total_amount")))
    (is (= "user" (onto-map/normalize-term "is_user_active")))
    (is (= "product" (onto-map/normalize-term "has_product_info")))
    (is (= "simple" (onto-map/normalize-term "simple")))
    (is (= "test" (onto-map/normalize-term "test___multiple")))
    (is (= "" (onto-map/normalize-term "_")))))

(deftest test-create-concept-similarity-map
  (testing "Concept similarity map creation"
    (let [sim-map (onto-map/create-concept-similarity-map)]
      (is (map? sim-map))
      (is (contains? sim-map :order))
      (is (contains? sim-map :customer))
      (is (contains? sim-map :product))
      (is (contains? sim-map :person))
      (is (contains? sim-map :organization))
      (is (vector? (:order sim-map)))
      (is (seq (:order sim-map))))))

(deftest test-match-concept-to-ontology
  (testing "Concept matching"
    (let [concept-map {}
          ontology-mappings {}
          result (onto-map/match-concept-to-ontology "order_id" concept-map ontology-mappings)]
      (is (map? result))
      (is (contains? result :concept))
      (is (contains? result :score))
      (is (number? (:score result)))
      (is (>= (:score result) 0))))
  
  (testing "Matching customer terms"
    (let [concept-map {}
          ontology-mappings {}
          result (onto-map/match-concept-to-ontology "customer_name" concept-map ontology-mappings)]
      (is (= :customer (:concept result)))
      (is (> (:score result) 0))))
  
  (testing "Matching product terms"
    (let [concept-map {}
          ontology-mappings {}
          result (onto-map/match-concept-to-ontology "product_sku" concept-map ontology-mappings)]
      (is (= :product (:concept result)))
      (is (> (:score result) 0)))))

(deftest test-analyze-domain-with-ontologies
  (testing "Domain analysis with organization data"
    (let [org-columns [{:property "http://example.org/employee_id"}
                       {:property "http://example.org/department"}
                       {:property "http://example.org/salary"}
                       {:property "http://example.org/manager"}
                       {:property "http://example.org/position"}]
          org-semantic-types [{:semanticType "currency"}]
          result (onto-map/analyze-domain-with-ontologies org-columns org-semantic-types)]
      (is (map? result))
      (is (contains? result :detected-domain))
      (is (contains? result :confidence))
      (is (contains? result :ontology-matches))
      (is (contains? result :semantic-indicators))
      (is (= :organization (:detected-domain result)))
      (is (number? (:confidence result)))
      (is (>= (:confidence result) 0))))
  
  (testing "Domain analysis with energy data"
    (let [energy-columns [{:property "http://example.org/meter_id"}
                          {:property "http://example.org/consumption_kwh"}
                          {:property "http://example.org/energy_source"}
                          {:property "http://example.org/grid_connection"}
                          {:property "http://example.org/power_demand"}]
          energy-semantic-types [{:semanticType "currency"}]
          result (onto-map/analyze-domain-with-ontologies energy-columns energy-semantic-types)]
      (is (= :energy (:detected-domain result)))
      (is (> (:confidence result) 0))))
  
  (testing "Domain analysis with food safety data"
    (let [food-columns [{:property "http://example.org/inspection_id"}
                        {:property "http://example.org/establishment_name"}
                        {:property "http://example.org/restaurant_type"}
                        {:property "http://example.org/food_handler_certified"}
                        {:property "http://example.org/violation_code"}]
          food-semantic-types [{:semanticType "email"}]
          result (onto-map/analyze-domain-with-ontologies food-columns food-semantic-types)]
      (is (= :food (:detected-domain result)))
      (is (> (:confidence result) 0))))
  
  (testing "Domain analysis with manufacturing data"
    (let [mfg-columns [{:property "http://example.org/batch_number"}
                       {:property "http://example.org/production_line"}
                       {:property "http://example.org/quality_standard"}
                       {:property "http://example.org/defect_count"}
                       {:property "http://example.org/inspection_date"}]
          mfg-semantic-types [{:semanticType "date"}]
          result (onto-map/analyze-domain-with-ontologies mfg-columns mfg-semantic-types)]
      (is (= :manufacturing (:detected-domain result)))
      (is (> (:confidence result) 0))))
  
  (testing "Domain analysis with healthcare data"
    (let [health-columns [{:property "http://example.org/patient_id"}
                          {:property "http://example.org/medical_record"}
                          {:property "http://example.org/diagnosis"}
                          {:property "http://example.org/treatment"}
                          {:property "http://example.org/physician"}]
          health-semantic-types [{:semanticType "ssn"}]
          result (onto-map/analyze-domain-with-ontologies health-columns health-semantic-types)]
      (is (= :healthcare (:detected-domain result)))
      (is (> (:confidence result) 0))))
  
  (testing "Empty data returns general domain"
    (let [result (onto-map/analyze-domain-with-ontologies [] [])]
      (is (= :general (:detected-domain result)))
      (is (= 0.0 (:confidence result))))))

(deftest test-suggest-schema-namespace
  (testing "Schema namespace suggestions"
    (let [domain-analysis {:detected-domain :sales :confidence 0.8}
          namespace (onto-map/suggest-schema-namespace domain-analysis)]
      (is (string? namespace))
      (is (.contains namespace "schema.org"))
      (is (.contains namespace "sales")))
    
    (let [domain-analysis {:detected-domain :energy :confidence 0.9}
          namespace (onto-map/suggest-schema-namespace domain-analysis)]
      (is (string? namespace))
      (is (.contains namespace "schema")))
    
    (let [domain-analysis {:detected-domain :unknown :confidence 0.1}
          namespace (onto-map/suggest-schema-namespace domain-analysis)]
      (is (string? namespace))
      (is (.contains namespace "schema.org")))))

(deftest test-generate-rdf-annotations
  (testing "RDF annotations generation"
    (let [columns [{:property "http://example.org/email" :semantic-type "email"}
                   {:property "http://example.org/phone" :semantic-type "phone"}
                   {:property "http://example.org/name" :semantic-type "text"}]
          annotations (onto-map/generate-rdf-annotations columns :customer)]
      (is (vector? annotations))
      (is (= 3 (count annotations)))
      (is (contains? (first annotations) :ontology-mapping))
      (is (contains? (first annotations) :ontology-source))))
  
  (testing "Email mapping"
    (let [columns [{:property "http://example.org/user_email" :semantic-type "email"}]
          annotations (onto-map/generate-rdf-annotations columns :customer)
          email-annotation (first annotations)]
      (is (= "email" (last (clojure.string/split (:ontology-mapping email-annotation) #"/"))))
      (is (= "Schema.org" (:ontology-source email-annotation)))))
  
  (testing "Phone mapping"
    (let [columns [{:property "http://example.org/contact_phone" :semantic-type "phone"}]
          annotations (onto-map/generate-rdf-annotations columns :customer)
          phone-annotation (first annotations)]
      (is (= "telephone" (last (clojure.string/split (:ontology-mapping phone-annotation) #"/"))))
      (is (= "Schema.org" (:ontology-source phone-annotation))))))

(deftest test-create-ontology-aware-config
  (testing "Ontology-aware configuration creation"
    (let [base-config {:existing "config"}
          domain-analysis {:detected-domain :sales
                          :confidence 0.85
                          :ontology-matches {:schema-org {:sales 5}}}
          result (onto-map/create-ontology-aware-config base-config domain-analysis)]
      (is (map? result))
      (is (= "config" (:existing result)))
      (is (contains? result :ontology))
      (is (= :sales (get-in result [:ontology :detected-domain])))
      (is (= 0.85 (get-in result [:ontology :confidence])))
      (is (string? (get-in result [:ontology :schema-namespace])))
      (is (set? (get-in result [:ontology :vocabularies-used]))))))

(deftest test-semantic-type-boosting
  (testing "Semantic type boosting in domain analysis"
    (let [columns [{:property "http://example.org/amount"}]
          semantic-types-with-currency [{:semanticType "currency"}]
          semantic-types-with-email [{:semanticType "email"}]
          
          result-currency (onto-map/analyze-domain-with-ontologies columns semantic-types-with-currency)
          result-email (onto-map/analyze-domain-with-ontologies columns semantic-types-with-email)]
      
      ; Currency should boost finance/sales domains
      (is (contains? #{:finance :sales} (:detected-domain result-currency)))
      
      ; Email should boost customer domain  
      (is (= :customer (:detected-domain result-email))))))

(deftest test-edge-cases
  (testing "Edge cases and error handling"
    (testing "Nil inputs"
      (let [result (onto-map/analyze-domain-with-ontologies nil nil)]
        (is (map? result))
        (is (= :general (:detected-domain result)))))
    
    (testing "Empty property URIs"
      (let [columns [{:property ""}]
            result (onto-map/analyze-domain-with-ontologies columns [])]
        (is (map? result))))
    
    (testing "Invalid semantic types"
      (let [columns [{:property "http://example.org/test"}]
            semantic-types [{:semanticType "invalid-type"}]
            result (onto-map/analyze-domain-with-ontologies columns semantic-types)]
        (is (map? result))))
    
    (testing "Very low confidence"
      (let [columns [{:property "http://example.org/random_field"}]
            result (onto-map/analyze-domain-with-ontologies columns [])]
        (is (number? (:confidence result)))
        (is (<= (:confidence result) 1.0))))))