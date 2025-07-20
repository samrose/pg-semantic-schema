(ns pg-semantic-schema.config-test
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.ontology OntModelSpec]
           [org.apache.jena.rdf.model ModelFactory]))

(deftest test-default-config-structure
  (testing "Default configuration has all required keys"
    (let [cfg config/*default-config*]
      (is (contains? cfg :jena))
      (is (contains? cfg :csv))
      (is (contains? cfg :postgres))
      (is (contains? cfg :semantic-types)))))

(deftest test-jena-config
  (testing "Jena configuration"
    (let [jena-cfg (:jena config/*default-config*)]
      (is (= :owl-micro (:reasoner-type jena-cfg)))
      (is (string? (:namespace-base jena-cfg)))
      (is (.startsWith (:namespace-base jena-cfg) "http://"))
      (is (instance? OntModelSpec (:model-spec jena-cfg))))))

(deftest test-csv-config
  (testing "CSV configuration"
    (let [csv-cfg (:csv config/*default-config*)]
      (is (char? (:delimiter csv-cfg)))
      (is (char? (:quote csv-cfg)))
      (is (char? (:escape csv-cfg)))
      (is (number? (:sample-size csv-cfg)))
      (is (> (:sample-size csv-cfg) 0)))))

(deftest test-postgres-config
  (testing "PostgreSQL configuration"
    (let [pg-cfg (:postgres config/*default-config*)]
      (is (string? (:schema-prefix pg-cfg)))
      (is (string? (:fact-table-suffix pg-cfg)))
      (is (string? (:dim-table-suffix pg-cfg)))
      (is (string? (:bridge-table-suffix pg-cfg))))))

(deftest test-semantic-types-config
  (testing "Semantic types configuration"
    (let [st-cfg (:semantic-types config/*default-config*)]
      (is (number? (:confidence-threshold st-cfg)))
      (is (<= 0 (:confidence-threshold st-cfg) 1))
      (is (map? (:patterns st-cfg)))
      (is (> (count (:patterns st-cfg)) 5)))))

(deftest test-semantic-type-patterns
  (testing "Semantic type regex patterns"
    (let [patterns (get-in config/*default-config* [:semantic-types :patterns])]
      (testing "Email pattern"
        (is (re-matches (:email patterns) "test@example.com"))
        (is (re-matches (:email patterns) "user.name+tag@domain.co.uk"))
        (is (not (re-matches (:email patterns) "invalid-email"))))
      
      (testing "Phone pattern"
        (is (re-matches (:phone patterns) "(555) 123-4567"))
        (is (re-matches (:phone patterns) "555-123-4567"))
        (is (re-matches (:phone patterns) "5551234567"))
        (is (not (re-matches (:phone patterns) "123"))))
      
      (testing "Currency pattern"
        (is (re-matches (:currency patterns) "$123.45"))
        (is (re-matches (:currency patterns) "123.45"))
        (is (re-matches (:currency patterns) "$1000"))
        (is (not (re-matches (:currency patterns) "abc"))))
      
      (testing "Date pattern"
        (is (re-matches (:date patterns) "2024-01-15"))
        (is (re-matches (:date patterns) "01/15/2024"))
        (is (re-matches (:date patterns) "01-15-2024"))
        (is (not (re-matches (:date patterns) "2024/1/1"))))
      
      (testing "SSN pattern"
        (is (re-matches (:ssn patterns) "123-45-6789"))
        (is (not (re-matches (:ssn patterns) "123456789"))))
      
      (testing "ZIP code pattern"
        (is (re-matches (:zip-code patterns) "12345"))
        (is (re-matches (:zip-code patterns) "12345-6789"))
        (is (not (re-matches (:zip-code patterns) "1234")))))))

(deftest test-create-ontology-model
  (testing "Ontology model creation"
    (let [model (config/create-ontology-model config/*default-config*)]
      (is (some? model))
      (is (>= (.size model) 0)))))

(deftest test-create-ontology-model-integration
  (testing "Ontology model integration with Jena"
    (let [model (config/create-ontology-model config/*default-config*)]
      (is (some? model))
      ; Test that we can add statements to the model
      (.add model
            (.createResource model "http://example.org/test")
            (.createProperty model "http://example.org/property")
            "test-value")
      (is (> (.size model) 0)))))