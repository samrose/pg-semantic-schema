(ns pg-semantic-schema.utils-test
  (:require [clojure.test :refer :all]
            [pg-semantic-schema.utils :as utils]))

(deftest test-detect-semantic-type
  (testing "Email detection"
    (is (= :email (utils/detect-semantic-type "user@example.com")))
    (is (= :email (utils/detect-semantic-type "test.email+tag@domain.co.uk")))
    (is (not= :email (utils/detect-semantic-type "not-an-email"))))
  
  (testing "Phone number detection"
    (is (= :phone (utils/detect-semantic-type "(555) 123-4567")))
    (is (= :phone (utils/detect-semantic-type "555-123-4567")))
    (is (= :phone (utils/detect-semantic-type "5551234567")))
    (is (not= :phone (utils/detect-semantic-type "123"))))
  
  (testing "Currency detection"
    (is (= :currency (utils/detect-semantic-type "$123.45")))
    (is (= :currency (utils/detect-semantic-type "1000.00")))
    (is (= :currency (utils/detect-semantic-type "$1,234.56")))
    (is (not= :currency (utils/detect-semantic-type "abc"))))
  
  (testing "Date detection"
    (is (= :date (utils/detect-semantic-type "2024-01-15")))
    (is (= :date (utils/detect-semantic-type "01/15/2024")))
    (is (= :date (utils/detect-semantic-type "01-15-2024")))
    (is (not= :date (utils/detect-semantic-type "2024/1/1"))))
  
  (testing "SSN detection"
    (is (= :ssn (utils/detect-semantic-type "123-45-6789")))
    (is (not= :ssn (utils/detect-semantic-type "123456789"))))
  
  (testing "URL detection"
    (is (= :url (utils/detect-semantic-type "https://example.com")))
    (is (= :url (utils/detect-semantic-type "http://test.org/path")))
    (is (not= :url (utils/detect-semantic-type "not-a-url"))))
  
  (testing "ZIP code detection"
    (is (= :zip-code (utils/detect-semantic-type "12345")))
    (is (= :zip-code (utils/detect-semantic-type "12345-6789")))
    (is (not= :zip-code (utils/detect-semantic-type "1234"))))
  
  (testing "Time detection"
    (is (= :time (utils/detect-semantic-type "14:30:00")))
    (is (= :time (utils/detect-semantic-type "09:15")))
    (is (not= :time (utils/detect-semantic-type "25:00"))))
  
  (testing "Unknown type for unmatched values"
    (is (= :unknown (utils/detect-semantic-type "random text")))
    (is (= :unknown (utils/detect-semantic-type "12345abc")))))

(deftest test-analyze-column-distribution
  (testing "Basic distribution analysis"
    (let [values ["A" "B" "A" "C" "B" "A"]
          result (utils/analyze-column-distribution values)]
      (is (= 3 (:unique-count result)))
      (is (= 6 (:total-count result)))
      (is (= 0.5 (:uniqueness result)))
      (is (= {"A" 3, "B" 2, "C" 1} (:value-counts result)))))
  
  (testing "All unique values"
    (let [values ["A" "B" "C" "D"]
          result (utils/analyze-column-distribution values)]
      (is (= 4 (:unique-count result)))
      (is (= 4 (:total-count result)))
      (is (= 1.0 (:uniqueness result)))))
  
  (testing "All same values"
    (let [values ["A" "A" "A" "A"]
          result (utils/analyze-column-distribution values)]
      (is (= 1 (:unique-count result)))
      (is (= 4 (:total-count result)))
      (is (= 0.25 (:uniqueness result)))))
  
  (testing "Empty collection"
    (let [result (utils/analyze-column-distribution [])]
      (is (= 0 (:unique-count result)))
      (is (= 0 (:total-count result)))
      (is (= 0.0 (:uniqueness result))))))

(deftest test-calculate-column-entropy
  (testing "Entropy calculation"
    (let [values ["A" "A" "B" "B"]]
      (is (pos? (utils/calculate-column-entropy values))))
    
    (let [values ["A" "A" "A" "A"]]
      (is (= 0.0 (utils/calculate-column-entropy values))))
    
    (let [values ["A" "B" "C" "D"]]
      (is (> (utils/calculate-column-entropy values) 1.0)))))

(deftest test-detect-column-role
  (testing "Identifier detection"
    (is (= :identifier (utils/detect-column-role ["ID001" "ID002" "ID003"] 1.0)))
    (is (= :identifier (utils/detect-column-role ["1" "2" "3"] 1.0))))
  
  (testing "Categorical dimension detection"
    (is (= :categorical-dimension (utils/detect-column-role ["Red" "Blue" "Green" "Red"] 0.75)))
    (is (= :categorical-dimension (utils/detect-column-role ["M" "F" "M" "F"] 0.5))))
  
  (testing "Measure detection"
    (is (= :measure (utils/detect-column-role ["100.5" "200.3" "150.7"] 1.0)))
    (is (= :measure (utils/detect-column-role ["$100" "$200" "$300"] 1.0))))
  
  (testing "Temporal dimension detection"
    (is (= :temporal-dimension (utils/detect-column-role ["2024-01-01" "2024-01-02" "2024-01-03"] 1.0))))
  
  (testing "General dimension for mixed data"
    (is (= :dimension (utils/detect-column-role ["A" "B" "C" "1" "2"] 1.0)))))

(deftest test-infer-data-relationships
  (testing "Functional dependency detection"
    (let [data [{"A" "1" "B" "X"}
                {"A" "1" "B" "X"}
                {"A" "2" "B" "Y"}
                {"A" "2" "B" "Y"}]
          relationships (utils/infer-data-relationships data)]
      (is (seq relationships))))
  
  (testing "Empty data"
    (let [relationships (utils/infer-data-relationships [])]
      (is (empty? relationships)))))

(deftest test-detect-foreign-key-candidates
  (testing "Foreign key detection"
    (let [data [{"customer_id" "C001" "order_id" "O001"}
                {"customer_id" "C002" "order_id" "O002"}]
          candidates (utils/detect-foreign-key-candidates data)]
      (is (coll? candidates))))
  
  (testing "No foreign keys in simple data"
    (let [data [{"name" "John" "age" "25"}
                {"name" "Jane" "age" "30"}]
          candidates (utils/detect-foreign-key-candidates data)]
      (is (coll? candidates)))))

(deftest test-calculate-data-quality-score
  (testing "Data quality calculation"
    (let [values ["A" "B" "C" "" nil]
          score (utils/calculate-data-quality-score values)]
      (is (number? score))
      (is (<= 0 score 1))))
  
  (testing "Perfect data quality"
    (let [values ["A" "B" "C"]
          score (utils/calculate-data-quality-score values)]
      (is (= 1.0 score))))
  
  (testing "Poor data quality"
    (let [values ["" nil "" nil]
          score (utils/calculate-data-quality-score values)]
      (is (= 0.0 score)))))

(deftest test-analyze-semantic-patterns
  (testing "Pattern analysis on sample data"
    (let [data [{"email" "user@test.com" "phone" "555-1234" "amount" "$100.50"}
                {"email" "test@example.org" "phone" "555-5678" "amount" "$200.75"}]
          patterns (utils/analyze-semantic-patterns data)]
      (is (map? patterns))
      (is (contains? patterns "email"))
      (is (contains? patterns "phone"))
      (is (contains? patterns "amount"))))
  
  (testing "Empty data"
    (let [patterns (utils/analyze-semantic-patterns [])]
      (is (map? patterns))
      (is (empty? patterns)))))

(deftest test-edge-cases
  (testing "Null and empty handling"
    (is (= :unknown (utils/detect-semantic-type nil)))
    (is (= :unknown (utils/detect-semantic-type "")))
    (is (= :unknown (utils/detect-semantic-type "   "))))
  
  (testing "Large values"
    (let [large-string (apply str (repeat 1000 "a"))]
      (is (= :unknown (utils/detect-semantic-type large-string)))))
  
  (testing "Special characters"
    (is (= :unknown (utils/detect-semantic-type "!@#$%^&*()")))))