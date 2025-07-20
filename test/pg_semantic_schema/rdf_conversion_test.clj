(ns pg-semantic-schema.rdf-conversion-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [pg-semantic-schema.rdf-conversion :as rdf]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.rdf.model ModelFactory]))

(def test-csv-content
  "id,name,email,phone,amount,date\n1,John Doe,john@test.com,(555) 123-4567,$100.50,2024-01-15\n2,Jane Smith,jane@test.com,(555) 987-6543,$200.75,2024-01-16")

(def test-csv-file "test-data.csv")

(defn create-test-csv []
  (spit test-csv-file test-csv-content))

(defn cleanup-test-csv []
  (when (.exists (io/file test-csv-file))
    (.delete (io/file test-csv-file))))

(use-fixtures :each
  (fn [f]
    (create-test-csv)
    (f)
    (cleanup-test-csv)))

(deftest test-parse-csv
  (testing "CSV parsing with valid file"
    (let [data (rdf/parse-csv test-csv-file ","")]
      (is (vector? data))
      (is (= 2 (count data)))
      (is (map? (first data)))
      (is (contains? (first data) "id"))
      (is (contains? (first data) "name"))
      (is (contains? (first data) "email"))))
  
  (testing "CSV headers extraction"
    (let [data (rdf/parse-csv test-csv-file ",")
          headers (keys (first data))]
      (is (contains? (set headers) "id"))
      (is (contains? (set headers) "name"))
      (is (contains? (set headers) "email"))
      (is (contains? (set headers) "phone"))
      (is (contains? (set headers) "amount"))
      (is (contains? (set headers) "date"))))
  
  (testing "CSV data values"
    (let [data (rdf/parse-csv test-csv-file ",")
          first-row (first data)
          second-row (second data)]
      (is (= "1" (get first-row "id")))
      (is (= "John Doe" (get first-row "name")))
      (is (= "john@test.com" (get first-row "email")))
      (is (= "2" (get second-row "id")))
      (is (= "Jane Smith" (get second-row "name")))))
  
  (testing "Nonexistent file"
    (is (thrown? Exception (rdf/parse-csv "nonexistent.csv" ",")))))

(deftest test-analyze-columns
  (testing "Column analysis"
    (let [data (rdf/parse-csv test-csv-file ",")
          analysis (rdf/analyze-columns data)]
      (is (map? analysis))
      (is (contains? analysis "id"))
      (is (contains? analysis "email"))
      (is (contains? analysis "phone"))
      (is (contains? analysis "amount"))
      
      (let [email-analysis (get analysis "email")]
        (is (contains? email-analysis :semantic-type))
        (is (contains? email-analysis :unique-values))
        (is (contains? email-analysis :null-values))
        (is (contains? email-analysis :role))
        (is (= :email (:semantic-type email-analysis))))
      
      (let [phone-analysis (get analysis "phone")]
        (is (= :phone (:semantic-type phone-analysis))))
      
      (let [amount-analysis (get analysis "amount")]
        (is (= :currency (:semantic-type amount-analysis))))
      
      (let [date-analysis (get analysis "date")]
        (is (= :date (:semantic-type date-analysis))))))
  
  (testing "Column roles detection"
    (let [data (rdf/parse-csv test-csv-file ",")
          analysis (rdf/analyze-columns data)]
      (is (= :identifier (get-in analysis ["id" :role])))
      (is (contains? #{:dimension :categorical-dimension} (get-in analysis ["name" :role])))
      (is (= :measure (get-in analysis ["amount" :role])))))
  
  (testing "Empty data"
    (let [analysis (rdf/analyze-columns [])]
      (is (map? analysis))
      (is (empty? analysis)))))

(deftest test-detect-potential-relationships
  (testing "Relationship detection with foreign key patterns"
    (let [data [{"customer_id" "C001" "order_id" "O001"}
                {"customer_id" "C002" "order_id" "O002"}]
          relationships (rdf/detect-potential-relationships data)]
      (is (vector? relationships))))
  
  (testing "Relationship detection with simple data"
    (let [data (rdf/parse-csv test-csv-file ",")
          relationships (rdf/detect-potential-relationships data)]
      (is (vector? relationships))))
  
  (testing "Empty data relationships"
    (let [relationships (rdf/detect-potential-relationships [])]
      (is (vector? relationships))
      (is (empty? relationships)))))

(deftest test-create-rdf-triples
  (testing "RDF triple creation"
    (let [data (rdf/parse-csv test-csv-file ",")
          column-analysis (rdf/analyze-columns data)
          relationships (rdf/detect-potential-relationships data)
          config config/*default-config*
          triples (rdf/create-rdf-triples data column-analysis relationships config)]
      (is (vector? triples))
      (is (seq triples))
      (is (every? map? triples))
      (is (every? #(contains? % :subject) triples))
      (is (every? #(contains? % :predicate) triples))
      (is (every? #(contains? % :object) triples))))
  
  (testing "Triple structure"
    (let [data (rdf/parse-csv test-csv-file ",")
          column-analysis (rdf/analyze-columns data)
          relationships (rdf/detect-potential-relationships data)
          config config/*default-config*
          triples (rdf/create-rdf-triples data column-analysis relationships config)
          first-triple (first triples)]
      (is (string? (:subject first-triple)))
      (is (string? (:predicate first-triple)))
      (is (contains? first-triple :object))))
  
  (testing "Namespace usage in triples"
    (let [data (rdf/parse-csv test-csv-file ",")
          column-analysis (rdf/analyze-columns data)
          relationships (rdf/detect-potential-relationships data)
          config config/*default-config*
          triples (rdf/create-rdf-triples data column-analysis relationships config)]
      (is (some #(.startsWith (:subject %) (:namespace-base (:jena config))) triples))
      (is (some #(.startsWith (:predicate %) (:namespace-base (:jena config))) triples)))))

(deftest test-csv-to-rdf
  (testing "Complete CSV to RDF conversion"
    (let [model (rdf/csv->rdf test-csv-file config/*default-config*)]
      (is (some? model))
      (is (> (.size model) 0))))
  
  (testing "RDF model contains expected statements"
    (let [model (rdf/csv->rdf test-csv-file config/*default-config*)
          statements (.listStatements model)]
      (is (.hasNext statements))
      (let [statement (.next statements)]
        (is (some? (.getSubject statement)))
        (is (some? (.getPredicate statement)))
        (is (some? (.getObject statement))))))
  
  (testing "RDF model with different sample size"
    (let [small-config (assoc-in config/*default-config* [:csv :sample-size] 1)
          model (rdf/csv->rdf test-csv-file small-config)]
      (is (some? model))
      (is (> (.size model) 0))))
  
  (testing "Nonexistent file throws exception"
    (is (thrown? Exception (rdf/csv->rdf "nonexistent.csv" config/*default-config*)))))

(deftest test-model-serialization
  (testing "RDF model can be serialized"
    (let [model (rdf/csv->rdf test-csv-file config/*default-config*)
          turtle-output (java.io.StringWriter.)]
      (.write model turtle-output "TURTLE")
      (is (> (count (.toString turtle-output)) 0))
      (is (.contains (.toString turtle-output) "@prefix")))))

(deftest test-complex-csv-data
  (testing "CSV with special characters and edge cases"
    (let [complex-csv "test-complex.csv"
          complex-content "id,description,price,notes\n1,\"Product with, comma\",$99.99,\"Notes with \"\"quotes\"\"\"\n2,Product 2,$199.50,Simple notes"]
      (spit complex-csv complex-content)
      (try
        (let [data (rdf/parse-csv complex-csv ",")]
          (is (= 2 (count data)))
          (is (= "Product with, comma" (get (first data) "description")))
          (is (= "Notes with \"quotes\"" (get (first data) "notes"))))
        (finally
          (.delete (io/file complex-csv))))))
  
  (testing "CSV with empty values"
    (let [empty-csv "test-empty.csv"
          empty-content "id,name,email\n1,John,john@test.com\n2,,\n3,Jane,jane@test.com"]
      (spit empty-csv empty-content)
      (try
        (let [data (rdf/parse-csv empty-csv ",")
              analysis (rdf/analyze-columns data)]
          (is (= 3 (count data)))
          (is (contains? analysis "name"))
          (is (> (get-in analysis ["name" :null-values]) 0)))
        (finally
          (.delete (io/file empty-csv))))))
  
  (testing "CSV with different delimiters"
    (let [pipe-csv "test-pipe.csv"
          pipe-content "id|name|email\n1|John|john@test.com\n2|Jane|jane@test.com"]
      (spit pipe-csv pipe-content)
      (try
        (let [data (rdf/parse-csv pipe-csv "|")]
          (is (= 2 (count data)))
          (is (= "John" (get (first data) "name"))))
        (finally
          (.delete (io/file pipe-csv)))))))

(deftest test-semantic-type-coverage
  (testing "All semantic types are detected"
    (let [semantic-csv "test-semantic.csv"
          semantic-content "email,phone,ssn,url,zip,currency,date,time\ntest@example.com,(555) 123-4567,123-45-6789,https://example.com,12345,$100.50,2024-01-15,14:30:00"]
      (spit semantic-csv semantic-content)
      (try
        (let [data (rdf/parse-csv semantic-csv ",")
              analysis (rdf/analyze-columns data)]
          (is (= :email (get-in analysis ["email" :semantic-type])))
          (is (= :phone (get-in analysis ["phone" :semantic-type])))
          (is (= :ssn (get-in analysis ["ssn" :semantic-type])))
          (is (= :url (get-in analysis ["url" :semantic-type])))
          (is (= :zip-code (get-in analysis ["zip" :semantic-type])))
          (is (= :currency (get-in analysis ["currency" :semantic-type])))
          (is (= :date (get-in analysis ["date" :semantic-type])))
          (is (= :time (get-in analysis ["time" :semantic-type]))))
        (finally
          (.delete (io/file semantic-csv)))))))

(deftest test-large-dataset-handling
  (testing "Large dataset processing"
    (let [large-csv "test-large.csv"
          ; Create a larger dataset
          large-content (str "id,name,value\n"
                           (apply str (for [i (range 1000)]
                                      (str i ",Name" i ",$" i ".00\n"))))]
      (spit large-csv large-content)
      (try
        (let [config-small-sample (assoc-in config/*default-config* [:csv :sample-size] 100)
              model (rdf/csv->rdf large-csv config-small-sample)]
          (is (some? model))
          (is (> (.size model) 0)))
        (finally
          (.delete (io/file large-csv)))))))

(deftest test-error-handling
  (testing "Malformed CSV handling"
    (let [malformed-csv "test-malformed.csv"
          malformed-content "id,name\n1,John\n2,Jane,Extra,Columns\n3"]
      (spit malformed-csv malformed-content)
      (try
        ; Should still process what it can
        (let [data (rdf/parse-csv malformed-csv ",")]
          (is (vector? data))
          (is (> (count data) 0)))
        (finally
          (.delete (io/file malformed-csv))))))
  
  (testing "Empty CSV file"
    (let [empty-csv "test-empty-file.csv"]
      (spit empty-csv "")
      (try
        (is (thrown? Exception (rdf/parse-csv empty-csv ",")))
        (finally
          (.delete (io/file empty-csv))))))
  
  (testing "CSV with only headers"
    (let [headers-only-csv "test-headers-only.csv"]
      (spit headers-only-csv "id,name,email")
      (try
        (let [data (rdf/parse-csv headers-only-csv ",")]
          (is (vector? data))
          (is (empty? data)))
        (finally
          (.delete (io/file headers-only-csv)))))))