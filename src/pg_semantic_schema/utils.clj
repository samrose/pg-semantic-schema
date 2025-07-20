(ns pg-semantic-schema.utils
  "Utility functions for semantic type detection and data analysis"
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [java-time :as time]
            [taoensso.timbre :as log])
  (:import [java.util.regex Pattern]
           [java.text NumberFormat DecimalFormat]
           [java.util Locale]))

(def semantic-patterns
  "Enhanced semantic type detection patterns"
  {:email {:pattern #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
           :confidence-base 0.9
           :description "Email address"}
   
   :phone {:pattern #"^(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$"
           :confidence-base 0.85
           :description "Phone number (US format)"}
   
   :phone-intl {:pattern #"^(\+[1-9]{1}[0-9]{1,3})?[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,9}$"
                :confidence-base 0.8
                :description "International phone number"}
   
   :currency {:pattern #"^[\$€£¥]?\d{1,3}(,\d{3})*(\.\d{2})?$"
              :confidence-base 0.9
              :description "Currency amount"}
   
   :currency-code {:pattern #"^[A-Z]{3}$"
                   :confidence-base 0.7
                   :description "ISO currency code"}
   
   :date-iso {:pattern #"^\d{4}-\d{2}-\d{2}$"
              :confidence-base 0.95
              :description "ISO date format (YYYY-MM-DD)"}
   
   :date-us {:pattern #"^\d{1,2}/\d{1,2}/\d{4}$"
             :confidence-base 0.85
             :description "US date format (MM/DD/YYYY)"}
   
   :date-eu {:pattern #"^\d{1,2}\.\d{1,2}\.\d{4}$"
             :confidence-base 0.85
             :description "European date format (DD.MM.YYYY)"}
   
   :time-24h {:pattern #"^\d{2}:\d{2}(:\d{2})?$"
              :confidence-base 0.9
              :description "24-hour time format"}
   
   :time-12h {:pattern #"^\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM|am|pm)$"
              :confidence-base 0.9
              :description "12-hour time format"}
   
   :datetime-iso {:pattern #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$"
                  :confidence-base 0.95
                  :description "ISO datetime format"}
   
   :url {:pattern #"^https?://[^\s/$.?#].[^\s]*$"
         :confidence-base 0.9
         :description "HTTP/HTTPS URL"}
   
   :url-ftp {:pattern #"^ftp://[^\s/$.?#].[^\s]*$"
             :confidence-base 0.9
             :description "FTP URL"}
   
   :ip-v4 {:pattern #"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
           :confidence-base 0.95
           :description "IPv4 address"}
   
   :ip-v6 {:pattern #"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
           :confidence-base 0.95
           :description "IPv6 address"}
   
   :mac-address {:pattern #"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"
                 :confidence-base 0.95
                 :description "MAC address"}
   
   :zip-code-us {:pattern #"^\d{5}(-\d{4})?$"
                 :confidence-base 0.9
                 :description "US ZIP code"}
   
   :postal-code-ca {:pattern #"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$"
                    :confidence-base 0.9
                    :description "Canadian postal code"}
   
   :ssn-us {:pattern #"^\d{3}-\d{2}-\d{4}$"
            :confidence-base 0.95
            :description "US Social Security Number"}
   
   :credit-card {:pattern #"^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$"
                 :confidence-base 0.8
                 :description "Credit card number"}
   
   :isbn-10 {:pattern #"^(?:\d{9}X|\d{10})$"
             :confidence-base 0.9
             :description "ISBN-10"}
   
   :isbn-13 {:pattern #"^\d{13}$"
             :confidence-base 0.85
             :description "ISBN-13"}
   
   :uuid {:pattern #"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
          :confidence-base 0.95
          :description "UUID"}
   
   :hex-color {:pattern #"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$"
               :confidence-base 0.9
               :description "Hexadecimal color code"}
   
   :latitude {:pattern #"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$"
              :confidence-base 0.85
              :description "Latitude coordinate"}
   
   :longitude {:pattern #"^[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$"
               :confidence-base 0.85
               :description "Longitude coordinate"}
   
   :vin {:pattern #"^[A-HJ-NPR-Z0-9]{17}$"
         :confidence-base 0.9
         :description "Vehicle Identification Number"}
   
   :stock-symbol {:pattern #"^[A-Z]{1,5}$"
                  :confidence-base 0.7
                  :description "Stock ticker symbol"}
   
   :percentage {:pattern #"^\d+(\.\d+)?%$"
                :confidence-base 0.9
                :description "Percentage value"}})

(defn detect-semantic-type-advanced
  "Advanced semantic type detection with confidence scoring"
  [value]
  (when (and value (string? value) (not (str/blank? value)))
    (let [trimmed (str/trim value)
          detections (keep (fn [[type-key pattern-info]]
                            (when (re-matches (:pattern pattern-info) trimmed)
                              {:type type-key
                               :confidence (:confidence-base pattern-info)
                               :description (:description pattern-info)}))
                          semantic-patterns)]
      
      ;; Return the detection with highest confidence
      (when (seq detections)
        (apply max-key :confidence detections)))))

(defn analyze-column-patterns
  "Analyze patterns in a column of data"
  [column-values]
  (let [non-null-values (remove str/blank? column-values)
        total-count (count column-values)
        non-null-count (count non-null-values)]
    
    (when (pos? non-null-count)
      (let [;; Detect semantic types
            semantic-detections (frequencies 
                                (keep #(when-let [detection (detect-semantic-type-advanced %)]
                                        (:type detection))
                                      non-null-values))
            
            ;; Analyze string patterns
            string-patterns (when (every? string? non-null-values)
                             {:min-length (apply min (map count non-null-values))
                              :max-length (apply max (map count non-null-values))
                              :avg-length (/ (reduce + (map count non-null-values)) 
                                           non-null-count)
                              :has-spaces (some #(str/includes? % " ") non-null-values)
                              :has-special-chars (some #(re-find #"[^a-zA-Z0-9\s]" %) non-null-values)
                              :all-uppercase (every? #(= % (str/upper-case %)) non-null-values)
                              :all-lowercase (every? #(= % (str/lower-case %)) non-null-values)})
            
            ;; Analyze numeric patterns (for strings that could be numbers)
            numeric-patterns (let [potential-numbers (filter #(re-matches #"[\d,.\-+$€£¥%]+.*" %) non-null-values)]
                              (when (seq potential-numbers)
                                {:numeric-ratio (/ (count potential-numbers) non-null-count)
                                 :has-currency-symbols (some #(re-find #"[\$€£¥]" %) potential-numbers)
                                 :has-percentages (some #(str/ends-with? % "%") potential-numbers)
                                 :has-negatives (some #(str/starts-with? % "-") potential-numbers)}))
            
            ;; Uniqueness analysis
            unique-values (distinct non-null-values)
            uniqueness-ratio (/ (count unique-values) non-null-count)
            
            ;; Most common semantic type
            primary-semantic-type (when (seq semantic-detections)
                                   (key (apply max-key val semantic-detections)))
            
            semantic-confidence (when primary-semantic-type
                                 (/ (get semantic-detections primary-semantic-type)
                                    non-null-count))]
        
        {:semantic-type primary-semantic-type
         :semantic-confidence semantic-confidence
         :semantic-detections semantic-detections
         :string-patterns string-patterns
         :numeric-patterns numeric-patterns
         :uniqueness-ratio uniqueness-ratio
         :null-ratio (/ (- total-count non-null-count) total-count)
         :sample-values (take 5 unique-values)}))))

(defn detect-functional-dependencies
  "Detect functional dependencies between columns"
  [data headers]
  (let [indexed-data (map-indexed vector data)
        dependencies (atom [])]
    
    ;; For each pair of columns, check if one determines the other
    (doseq [i (range (count headers))
            j (range (count headers))
            :when (not= i j)]
      
      (let [col-i-name (nth headers i)
            col-j-name (nth headers j)
            
            ;; Group by column i values and check if column j values are consistent
            groups (group-by #(nth % i) data)
            
            ;; Check if each group has only one unique value for column j
            is-functional-dependency? 
            (every? (fn [[group-key rows]]
                     (let [col-j-values (map #(nth % j) rows)
                           unique-j-values (distinct (remove str/blank? col-j-values))]
                       (<= (count unique-j-values) 1)))
                   groups)
            
            ;; Calculate strength of dependency
            dependency-strength
            (if is-functional-dependency?
              1.0
              (let [total-violations 
                    (reduce + (map (fn [[group-key rows]]
                                   (let [col-j-values (map #(nth % j) rows)
                                         unique-j-values (distinct (remove str/blank? col-j-values))]
                                     (max 0 (- (count unique-j-values) 1))))
                                 groups))]
                (- 1.0 (/ total-violations (count data)))))]
        
        (when (> dependency-strength 0.8)
          (swap! dependencies conj
                {:determinant col-i-name
                 :dependent col-j-name
                 :strength dependency-strength
                 :type (if is-functional-dependency? :full :partial)}))))
    
    @dependencies))

(defn detect-inclusion-dependencies
  "Detect inclusion dependencies (foreign key candidates)"
  [data headers]
  (let [dependencies (atom [])]
    
    (doseq [i (range (count headers))
            j (range (count headers))
            :when (not= i j)]
      
      (let [col-i-values (set (map #(nth % i) data))
            col-j-values (set (map #(nth % j) data))
            
            ;; Remove null/empty values
            clean-i-values (set (remove str/blank? col-i-values))
            clean-j-values (set (remove str/blank? col-j-values))
            
            ;; Check inclusion relationships
            i-subset-of-j? (set/subset? clean-i-values clean-j-values)
            j-subset-of-i? (set/subset? clean-j-values clean-i-values)
            
            ;; Calculate overlap
            intersection (set/intersection clean-i-values clean-j-values)
            union (set/union clean-i-values clean-j-values)
            jaccard-similarity (if (seq union)
                                (/ (count intersection) (count union))
                                0)]
        
        (when (or i-subset-of-j? j-subset-of-i? (> jaccard-similarity 0.7))
          (swap! dependencies conj
                {:from-column (nth headers i)
                 :to-column (nth headers j)
                 :relationship-type (cond
                                     i-subset-of-j? :subset
                                     j-subset-of-i? :superset
                                     :else :overlap)
                 :jaccard-similarity jaccard-similarity
                 :shared-values (count intersection)}))))
    
    @dependencies))

(defn infer-cardinality-relationships
  "Infer cardinality relationships between columns"
  [data headers functional-deps inclusion-deps]
  (mapv (fn [fd]
          (let [determinant-col (:determinant fd)
                dependent-col (:dependent fd)
                
                ;; Count unique values for each column
                det-idx (.indexOf headers determinant-col)
                dep-idx (.indexOf headers dependent-col)
                
                det-values (distinct (map #(nth % det-idx) data))
                dep-values (distinct (map #(nth % dep-idx) data))
                
                ;; Analyze cardinality
                cardinality (cond
                             (and (= (count det-values) (count data))
                                  (= (count dep-values) (count data))) :one-to-one
                             (< (count det-values) (count dep-values)) :one-to-many
                             (> (count det-values) (count dep-values)) :many-to-one
                             :else :many-to-many)]
            
            (assoc fd :cardinality cardinality)))
        functional-deps))

(defn suggest-data-types
  "Suggest appropriate data types based on column analysis"
  [column-analysis]
  (let [{:keys [semantic-type string-patterns numeric-patterns uniqueness-ratio]} column-analysis]
    
    (cond
      ;; Semantic type based suggestions
      semantic-type
      (case semantic-type
        :email "VARCHAR(255)"
        :phone "VARCHAR(20)"
        :phone-intl "VARCHAR(30)"
        :currency "DECIMAL(15,2)"
        :currency-code "CHAR(3)"
        (:date-iso :date-us :date-eu) "DATE"
        (:time-24h :time-12h) "TIME"
        :datetime-iso "TIMESTAMP"
        (:url :url-ftp) "TEXT"
        (:ip-v4 :ip-v6) "INET"
        :mac-address "MACADDR"
        (:zip-code-us :postal-code-ca) "VARCHAR(10)"
        :ssn-us "CHAR(11)"
        :credit-card "CHAR(19)"
        (:isbn-10 :isbn-13) "VARCHAR(17)"
        :uuid "UUID"
        :hex-color "CHAR(7)"
        (:latitude :longitude) "DECIMAL(10,8)"
        :vin "CHAR(17)"
        :stock-symbol "VARCHAR(10)"
        :percentage "DECIMAL(5,2)"
        "TEXT")
      
      ;; High uniqueness suggests identifier
      (> uniqueness-ratio 0.9)
      (if (and string-patterns (< (:max-length string-patterns) 50))
        (str "VARCHAR(" (:max-length string-patterns) ")")
        "TEXT")
      
      ;; Numeric patterns
      (and numeric-patterns (> (:numeric-ratio numeric-patterns) 0.8))
      (cond
        (:has-currency-symbols numeric-patterns) "DECIMAL(15,2)"
        (:has-percentages numeric-patterns) "DECIMAL(5,2)"
        :else "NUMERIC")
      
      ;; String patterns
      string-patterns
      (cond
        (:all-uppercase string-patterns) (str "CHAR(" (:max-length string-patterns) ")")
        (< (:max-length string-patterns) 255) (str "VARCHAR(" (:max-length string-patterns) ")")
        :else "TEXT")
      
      ;; Default
      :else "TEXT")))

(defn calculate-column-statistics
  "Calculate comprehensive statistics for a column"
  [column-values]
  (let [non-null-values (remove str/blank? column-values)
        total-count (count column-values)
        non-null-count (count non-null-values)]
    
    (merge
     {:total-count total-count
      :non-null-count non-null-count
      :null-count (- total-count non-null-count)
      :null-percentage (/ (- total-count non-null-count) total-count)
      :unique-count (count (distinct non-null-values))
      :uniqueness-ratio (/ (count (distinct non-null-values)) non-null-count)}
     
     ;; String statistics
     (when (every? string? non-null-values)
       {:min-length (apply min (map count non-null-values))
        :max-length (apply max (map count non-null-values))
        :avg-length (/ (reduce + (map count non-null-values)) non-null-count)})
     
     ;; Numeric statistics (for parseable numbers)
     (let [numeric-values (keep (fn [v]
                                 (try (Double/parseDouble (str/replace v #"[,$%]" ""))
                                      (catch Exception _ nil)))
                               non-null-values)]
       (when (seq numeric-values)
         {:numeric-count (count numeric-values)
          :numeric-ratio (/ (count numeric-values) non-null-count)
          :min-value (apply min numeric-values)
          :max-value (apply max numeric-values)
          :avg-value (/ (reduce + numeric-values) (count numeric-values))})))))

(comment
  ;; Example usage for REPL exploration
  
  ;; Test semantic type detection
  (detect-semantic-type-advanced "john.doe@example.com")
  (detect-semantic-type-advanced "$1,234.56")
  (detect-semantic-type-advanced "2023-12-25")
  
  ;; Test column analysis
  (analyze-column-patterns ["john@example.com" "jane@test.org" "bob@company.net"])
  (analyze-column-patterns ["$100.00" "$250.50" "$1,500.25"])
  (analyze-column-patterns ["2023-01-01" "2023-01-02" "2023-01-03"])
  
  ;; Test data type suggestions
  (suggest-data-types {:semantic-type :email})
  (suggest-data-types {:uniqueness-ratio 0.95 :string-patterns {:max-length 20}}))