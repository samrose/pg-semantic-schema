(ns pg-semantic-schema.sql-generation-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [pg-semantic-schema.sql-generation :as sql]
            [pg-semantic-schema.config :as config]))

;; Sample test data
(def sample-column-data
  {:property "http://example.org/sales/customer_id"
   :role :identifier
   :semantic-type :unknown
   :uniqueValues "100"
   :nullValues "5"
   :data-type "http://www.w3.org/2001/XMLSchema#string"})

(def sample-fact-structure
  {:table-name "sales_fact"
   :measures [{:property "http://example.org/sales/amount" :role :measure :semantic-type :currency}
             {:property "http://example.org/sales/quantity" :role :measure :semantic-type :integer}]
   :dimension-references [{:property "http://example.org/sales/customer_id" :role :dimension :semantic-type :unknown}
                         {:property "http://example.org/sales/product_id" :role :dimension :semantic-type :unknown}
                         {:property "http://example.org/sales/date" :role :temporal-dimension :semantic-type :date}]
   :natural-keys [{:property "http://example.org/sales/id" :role :identifier :semantic-type :unknown}]
   :foreign-key-candidates [{:sourceColumn "customer_id" :targetColumn "customer_table_id"}]})

(def sample-dim-structure
  {:table-name "customer_dim"
   :natural-key {:property "http://example.org/customers/customer_id" :role :identifier :semantic-type :unknown}
   :attributes [{:property "http://example.org/customers/name" :role :dimension :semantic-type :unknown}
               {:property "http://example.org/customers/email" :role :dimension :semantic-type :email}
               {:property "http://example.org/customers/city" :role :categorical-dimension :semantic-type :unknown}]
   :measures []
   :hierarchies [{:parentColumn "state" :childColumn "city"}]})

(def sample-schema-analysis
  {:ontology {:column-roles [{:property "http://example.org/test/id" :role :identifier :semantic-type :unknown
                             :uniqueValues "100" :nullValues "0"}
                            {:property "http://example.org/test/name" :role :dimension :semantic-type :unknown
                             :uniqueValues "95" :nullValues "5"}
                            {:property "http://example.org/test/amount" :role :measure :semantic-type :currency
                             :uniqueValues "80" :nullValues "20"}]}
   :recommended-pattern {:schema-pattern :star
                        :central-table sample-fact-structure}})

(deftest test-semantic-type-mapping
  (testing "Email semantic type mapping"
    (is (= "VARCHAR(255)" (sql/semantic-type->postgres-type :email nil))))
  
  (testing "Phone semantic type mapping"
    (is (= "VARCHAR(20)" (sql/semantic-type->postgres-type :phone nil))))
  
  (testing "Currency semantic type mapping"
    (is (= "DECIMAL(15,2)" (sql/semantic-type->postgres-type :currency nil))))
  
  (testing "Date semantic type mapping"
    (is (= "DATE" (sql/semantic-type->postgres-type :date nil))))
  
  (testing "Time semantic type mapping"
    (is (= "TIME" (sql/semantic-type->postgres-type :time nil))))
  
  (testing "URL semantic type mapping"
    (is (= "TEXT" (sql/semantic-type->postgres-type :url nil))))
  
  (testing "ZIP code semantic type mapping"
    (is (= "VARCHAR(10)" (sql/semantic-type->postgres-type :zip-code nil))))
  
  (testing "SSN semantic type mapping"
    (is (= "CHAR(11)" (sql/semantic-type->postgres-type :ssn nil))))
  
  (testing "XSD integer mapping fallback"
    (is (= "BIGINT" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#integer"))))
  
  (testing "XSD decimal mapping fallback"
    (is (= "DECIMAL(15,4)" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#decimal"))))
  
  (testing "XSD boolean mapping fallback"
    (is (= "BOOLEAN" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#boolean"))))
  
  (testing "XSD date mapping fallback"
    (is (= "DATE" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#date"))))
  
  (testing "XSD dateTime mapping fallback"
    (is (= "TIMESTAMP" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#dateTime"))))
  
  (testing "XSD time mapping fallback"
    (is (= "TIME" (sql/semantic-type->postgres-type :unknown "http://www.w3.org/2001/XMLSchema#time"))))
  
  (testing "Default TEXT mapping"
    (is (= "TEXT" (sql/semantic-type->postgres-type :unknown "unknown-type")))))

(deftest test-varchar-length-calculation
  (testing "Email semantic type length"
    (is (= 255 (sql/calculate-varchar-length :dimension 100 :email))))
  
  (testing "Phone semantic type length"
    (is (= 20 (sql/calculate-varchar-length :dimension 100 :phone))))
  
  (testing "ZIP code semantic type length"
    (is (= 10 (sql/calculate-varchar-length :dimension 100 :zip-code))))
  
  (testing "SSN semantic type length"
    (is (= 11 (sql/calculate-varchar-length :dimension 100 :ssn))))
  
  (testing "Identifier role-based sizing"
    (let [length (sql/calculate-varchar-length :identifier 25 :unknown)]
      (is (>= length 50))
      (is (<= length 255))))
  
  (testing "Categorical dimension role-based sizing"
    (let [length (sql/calculate-varchar-length :categorical-dimension 15 :unknown)]
      (is (>= length 50))
      (is (<= length 100))))
  
  (testing "Dimension role-based sizing"
    (let [length (sql/calculate-varchar-length :dimension 30 :unknown)]
      (is (>= length 100))
      (is (<= length 500))))
  
  (testing "Default sizing"
    (is (= 255 (sql/calculate-varchar-length :measure 50 :unknown)))))

(deftest test-column-definition-generation
  (testing "Basic column definition generation"
    (let [config config/*default-config*
          result (sql/generate-column-definition sample-column-data :general config)]
      (is (map? result))
      (is (contains? result :column-name))
      (is (contains? result :data-type))
      (is (contains? result :constraints))
      (is (contains? result :role))
      (is (contains? result :semantic-type))
      (is (contains? result :original-property))))
  
  (testing "Column name sanitization"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :property "http://example.org/sales/customer-id#test")
          result (sql/generate-column-definition col-data :general config)]
      (is (= "customer_id_test" (:column-name result)))
      (is (not (str/includes? (:column-name result) "http")))
      (is (not (str/includes? (:column-name result) "#")))
      (is (not (str/includes? (:column-name result) "-")))))
  
  (testing "Currency semantic type mapping"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :semantic-type :currency)
          result (sql/generate-column-definition col-data :general config)]
      (is (= "DECIMAL(15,2)" (:data-type result)))))
  
  (testing "Email semantic type with VARCHAR adjustment"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :semantic-type :email)
          result (sql/generate-column-definition col-data :general config)]
      (is (= "VARCHAR(255)" (:data-type result)))))
  
  (testing "NOT NULL constraint generation"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :nullValues "1" :uniqueValues "99") ; < 10% nulls
          result (sql/generate-column-definition col-data :general config)]
      (is (some #(= "NOT NULL" %) (:constraints result)))))
  
  (testing "UNIQUE constraint generation for identifiers"
    (let [config config/*default-config*
          col-data (assoc sample-column-data 
                         :role :identifier
                         :semantic-type :unknown
                         :property "http://example.org/test/user_id")
          result (sql/generate-column-definition col-data :general config)]
      ; Should have UNIQUE constraint due to identifier role and 'id' in name
      (is (some #(= "UNIQUE" %) (:constraints result)))))
  
  (testing "No UNIQUE constraint for semantic types"
    (let [config config/*default-config*
          col-data (assoc sample-column-data 
                         :role :identifier
                         :semantic-type :email
                         :property "http://example.org/test/user_id")
          result (sql/generate-column-definition col-data :general config)]
      ; Should NOT have UNIQUE constraint due to semantic type not being :unknown
      (is (not (some #(= "UNIQUE" %) (:constraints result)))))))

(deftest test-fact-table-ddl-generation
  (testing "Fact table DDL structure"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)]
      (is (map? result))
      (is (contains? result :table-ddl))
      (is (contains? result :indexes))
      (is (contains? result :table-name))
      (is (contains? result :columns))))
  
  (testing "Fact table DDL content"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)
          ddl (:table-ddl result)]
      (is (string? ddl))
      (is (str/includes? ddl "CREATE TABLE"))
      (is (str/includes? ddl "test_schema"))
      (is (str/includes? ddl "fact_id BIGSERIAL PRIMARY KEY"))
      (is (str/includes? ddl "sales_fact"))))
  
  (testing "Fact table surrogate key"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)
          columns (:columns result)
          surrogate-key (first (filter #(= (:role %) :surrogate-key) columns))]
      (is (some? surrogate-key))
      (is (= "fact_id" (:column-name surrogate-key)))
      (is (= "BIGSERIAL" (:data-type surrogate-key)))
      (is (some #(= "PRIMARY KEY" %) (:constraints surrogate-key)))))
  
  (testing "Fact table measure columns"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)
          columns (:columns result)
          measure-columns (filter #(= (:role %) :measure) columns)]
      (is (= 2 (count measure-columns)))))
  
  (testing "Fact table foreign key columns"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)
          columns (:columns result)
          fk-columns (filter #(= (:role %) :foreign-key) columns)]
      (is (= 3 (count fk-columns)))
      (is (every? #(str/ends-with? (:column-name %) "_key") fk-columns))
      (is (every? #(= "BIGINT" (:data-type %)) fk-columns))))
  
  (testing "Fact table indexes"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)
          indexes (:indexes result)]
      (is (vector? indexes))
      (is (= 2 (count indexes)))
      (is (every? string? indexes))
      (is (some #(str/includes? % "idx_") indexes)))))

(deftest test-dimension-table-ddl-generation
  (testing "Dimension table DDL structure"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)]
      (is (map? result))
      (is (contains? result :table-ddl))
      (is (contains? result :indexes))
      (is (contains? result :table-name))
      (is (contains? result :columns))))
  
  (testing "Dimension table DDL content"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          ddl (:table-ddl result)]
      (is (string? ddl))
      (is (str/includes? ddl "CREATE TABLE"))
      (is (str/includes? ddl "test_schema"))
      (is (str/includes? ddl "dim_id BIGSERIAL PRIMARY KEY"))
      (is (str/includes? ddl "dim_customer_dim"))))
  
  (testing "Dimension table surrogate key"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          columns (:columns result)
          surrogate-key (first (filter #(= (:role %) :surrogate-key) columns))]
      (is (some? surrogate-key))
      (is (= "dim_id" (:column-name surrogate-key)))
      (is (= "BIGSERIAL" (:data-type surrogate-key)))))
  
  (testing "Dimension table natural key"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          columns (:columns result)
          natural-key (first (filter #(= (:role %) :natural-key) columns))]
      (is (some? natural-key))
      (is (some #(= "NOT NULL" %) (:constraints natural-key)))
      (is (some #(= "UNIQUE" %) (:constraints natural-key)))))
  
  (testing "Dimension table SCD Type 2 columns"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          columns (:columns result)
          scd-columns (filter #(= (:role %) :scd-metadata) columns)]
      (is (= 3 (count scd-columns)))
      (is (some #(= "effective_date" (:column-name %)) scd-columns))
      (is (some #(= "expiry_date" (:column-name %)) scd-columns))
      (is (some #(= "is_current" (:column-name %)) scd-columns))))
  
  (testing "Dimension table attributes"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          columns (:columns result)
          attribute-columns (filter #(contains? #{:dimension :categorical-dimension} (:role %)) columns)]
      (is (= 3 (count attribute-columns)))))
  
  (testing "Dimension table indexes"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)
          indexes (:indexes result)]
      (is (vector? indexes))
      (is (= 2 (count indexes)))
      (is (every? string? indexes))
      (is (some #(str/includes? % "natural_key") indexes))
      (is (some #(str/includes? % "current") indexes)))))

(deftest test-snowflake-dimension-ddl-generation
  (testing "Snowflake dimension DDL structure"
    (let [config config/*default-config*
          snowflake-dims [{:parent-table {:name "country" :key-column "country_id"}
                          :child-table {:name "state" :key-column "state_id"}}]
          result (sql/generate-snowflake-dimension-ddl snowflake-dims "test_schema" config)]
      (is (sequential? result))
      (is (= 1 (count result)))
      (is (map? (first result)))
      (is (contains? (first result) :parent-table))
      (is (contains? (first result) :child-table))
      (is (contains? (first result) :foreign-key-constraint))))
  
  (testing "Snowflake dimension DDL content"
    (let [config config/*default-config*
          snowflake-dims [{:parent-table {:name "country" :key-column "country_id"}
                          :child-table {:name "state" :key-column "state_id"}}]
          result (sql/generate-snowflake-dimension-ddl snowflake-dims "test_schema" config)
          parent-ddl (:parent-table (first result))
          child-ddl (:child-table (first result))
          fk-constraint (:foreign-key-constraint (first result))]
      (is (string? (:table-ddl parent-ddl)))
      (is (string? (:table-ddl child-ddl)))
      (is (string? fk-constraint))
      (is (str/includes? fk-constraint "ALTER TABLE"))
      (is (str/includes? fk-constraint "ADD CONSTRAINT")))))

(deftest test-schema-ddl-generation
  (testing "Schema DDL generation"
    (let [result (sql/generate-schema-ddl "test_schema")]
      (is (sequential? result))
      (is (> (count result) 0))
      (is (some #(str/includes? % "CREATE SCHEMA") result))
      (is (some #(str/includes? % "SET search_path") result))))
  
  (testing "Schema DDL content"
    (let [result (sql/generate-schema-ddl "my_test_schema")]
      (is (some #(str/includes? % "my_test_schema") result)))))

(deftest test-comments-ddl-generation
  (testing "Comments DDL generation"
    (let [table-ddl {:table-name "test_table"
                    :columns [{:column-name "id" :semantic-type :unknown}
                             {:column-name "email" :semantic-type :email}
                             {:column-name "amount" :semantic-type :currency}]}
          result (sql/generate-comments-ddl table-ddl "test_schema")]
      (is (sequential? result))
      (is (> (count result) 0))
      (is (some #(str/includes? % "COMMENT ON TABLE") result))
      (is (some #(str/includes? % "COMMENT ON COLUMN") result))))
  
  (testing "Comments DDL content"
    (let [table-ddl {:table-name "test_table"
                    :columns [{:column-name "email" :semantic-type :email}]}
          result (sql/generate-comments-ddl table-ddl "test_schema")
          column-comment (first (filter #(str/includes? % "COMMENT ON COLUMN") result))]
      (is (some? column-comment))
      (is (str/includes? column-comment "test_schema.test_table.email"))
      (is (str/includes? column-comment "email")))))

(deftest test-maintenance-functions-generation
  (testing "Maintenance functions generation"
    (let [result (sql/generate-maintenance-functions "test_schema")]
      (is (sequential? result))
      (is (> (count result) 0))
      (is (some #(str/includes? % "CREATE OR REPLACE FUNCTION") result))
      (is (some #(str/includes? % "update_dimension_scd") result))
      (is (some #(str/includes? % "TRIGGER") result))))
  
  (testing "Maintenance functions content"
    (let [result (sql/generate-maintenance-functions "my_schema")]
      (is (some #(str/includes? % "my_schema") result)))))

(deftest test-main-ddl-generation
  (testing "Main DDL generation with star pattern"
    (let [config config/*default-config*
          result (sql/generate-ddl sample-schema-analysis "test_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0))
      (is (some #(str/includes? % "CREATE SCHEMA") result))
      (is (some #(str/includes? % "CREATE TABLE") result))
      (is (some #(str/includes? % "COMMENT ON") result))))
  
  (testing "Main DDL generation with dimension pattern"
    (let [config config/*default-config*
          dim-analysis {:ontology {:column-roles []}
                       :recommended-pattern {:schema-pattern :dimension-table
                                           :table-structure sample-dim-structure}}
          result (sql/generate-ddl dim-analysis "customer_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0))))
  
  (testing "Main DDL generation with snowflake pattern"
    (let [config config/*default-config*
          snowflake-analysis {:ontology {:column-roles []}
                             :recommended-pattern {:schema-pattern :snowflake
                                                 :central-table sample-fact-structure
                                                 :dimension-hierarchies [{:parent-table {:name "country"}
                                                                        :child-table {:name "state"}}]}}
          result (sql/generate-ddl snowflake-analysis "sales_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0))))
  
  (testing "Main DDL generation with default pattern"
    (let [config config/*default-config*
          default-analysis {:ontology {:column-roles [{:property "http://example.org/test/id" :role :identifier
                                                      :semantic-type :unknown :uniqueValues "100" :nullValues "0"}]}
                           :recommended-pattern {:schema-pattern :unknown}}
          result (sql/generate-ddl default-analysis "unknown_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0))
      (is (some #(str/includes? % "CREATE TABLE") result)))))

(deftest test-edge-cases
  (testing "Empty column roles"
    (let [config config/*default-config*
          empty-analysis {:ontology {:column-roles []}
                         :recommended-pattern {:schema-pattern :unknown}}
          result (sql/generate-ddl empty-analysis "empty_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0))))
  
  (testing "Invalid semantic types"
    (let [result (sql/semantic-type->postgres-type :invalid-type "invalid-data-type")]
      (is (= "TEXT" result))))
  
  (testing "Zero uniqueness and null values"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :uniqueValues "0" :nullValues "0")
          result (sql/generate-column-definition col-data :general config)]
      (is (map? result))
      (is (contains? result :column-name))))
  
  (testing "Very high unique count"
    (let [length (sql/calculate-varchar-length :identifier 1000 :unknown)]
      (is (<= length 255))))
  
  (testing "Very low unique count"
    (let [length (sql/calculate-varchar-length :categorical-dimension 1 :unknown)]
      (is (>= length 50))))
  
  (testing "Missing table structure"
    (let [config config/*default-config*
          analysis {:ontology {:column-roles []}
                   :recommended-pattern {:schema-pattern :dimension-table
                                       :table-structure nil}}
          result (sql/generate-ddl analysis "test_table" "test_schema" config)]
      (is (sequential? result))
      (is (> (count result) 0)))))

(deftest test-constraint-generation-logic
  (testing "UNIQUE constraint requirements"
    ; Test all conditions for UNIQUE constraint generation
    (let [config config/*default-config*]
      
      ; Should have UNIQUE - identifier role, contains "id", unknown semantic type
      (let [col-data {:property "http://example.org/test/user_id"
                     :role :identifier
                     :semantic-type :unknown
                     :uniqueValues "10"
                     :nullValues "0"}
            result (sql/generate-column-definition col-data :general config)]
        (is (some #(= "UNIQUE" %) (:constraints result))))
      
      ; Should NOT have UNIQUE - not identifier role
      (let [col-data {:property "http://example.org/test/user_id"
                     :role :dimension
                     :semantic-type :unknown
                     :uniqueValues "10"
                     :nullValues "0"}
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "UNIQUE" %) (:constraints result)))))
      
      ; Should NOT have UNIQUE - too few unique values
      (let [col-data {:property "http://example.org/test/user_id"
                     :role :identifier
                     :semantic-type :unknown
                     :uniqueValues "3"
                     :nullValues "0"}
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "UNIQUE" %) (:constraints result)))))
      
      ; Should NOT have UNIQUE - doesn't contain identifier keywords
      (let [col-data {:property "http://example.org/test/name"
                     :role :identifier
                     :semantic-type :unknown
                     :uniqueValues "10"
                     :nullValues "0"}
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "UNIQUE" %) (:constraints result)))))
      
      ; Should NOT have UNIQUE - semantic type is not unknown
      (let [col-data {:property "http://example.org/test/user_id"
                     :role :identifier
                     :semantic-type :email
                     :uniqueValues "10"
                     :nullValues "0"}
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "UNIQUE" %) (:constraints result)))))))
  
  (testing "NOT NULL constraint requirements"
    (let [config config/*default-config*]
      
      ; Should have NOT NULL - less than 10% nulls
      (let [col-data (assoc sample-column-data :uniqueValues "95" :nullValues "5")
            result (sql/generate-column-definition col-data :general config)]
        (is (some #(= "NOT NULL" %) (:constraints result))))
      
      ; Should NOT have NOT NULL - more than 10% nulls
      (let [col-data (assoc sample-column-data :uniqueValues "80" :nullValues "20")
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "NOT NULL" %) (:constraints result)))))
      
      ; Should NOT have NOT NULL - zero total values
      (let [col-data (assoc sample-column-data :uniqueValues "0" :nullValues "0")
            result (sql/generate-column-definition col-data :general config)]
        (is (not (some #(= "NOT NULL" %) (:constraints result))))))))

(deftest test-varchar-length-adjustments
  (testing "VARCHAR length adjustment for different types"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :semantic-type :email)
          result (sql/generate-column-definition col-data :general config)]
      (is (str/starts-with? (:data-type result) "VARCHAR"))
      (is (str/includes? (:data-type result) "255"))))
  
  (testing "Non-VARCHAR types remain unchanged"
    (let [config config/*default-config*
          col-data (assoc sample-column-data :semantic-type :currency)
          result (sql/generate-column-definition col-data :general config)]
      (is (= "DECIMAL(15,2)" (:data-type result)))))
  
  (testing "Role-based VARCHAR sizing"
    (let [identifier-length (sql/calculate-varchar-length :identifier 30 :unknown)
          categorical-length (sql/calculate-varchar-length :categorical-dimension 15 :unknown)
          dimension-length (sql/calculate-varchar-length :dimension 40 :unknown)]
      (is (>= identifier-length 50))
      (is (>= categorical-length 50))
      (is (>= dimension-length 100)))))

(deftest test-table-naming-conventions
  (testing "Fact table naming"
    (let [config config/*default-config*
          result (sql/generate-fact-table-ddl sample-fact-structure "test_schema" config)]
      (is (str/includes? (:table-name result) "semantic_"))
      (is (str/includes? (:table-name result) "_fact"))))
  
  (testing "Dimension table naming"
    (let [config config/*default-config*
          result (sql/generate-dimension-table-ddl sample-dim-structure "test_schema" config)]
      (is (str/starts-with? (:table-name result) "dim_"))
      (is (str/includes? (:table-name result) "customer_dim")))))