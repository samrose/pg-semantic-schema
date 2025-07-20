(ns pg-semantic-schema.rdf-conversion
  "CSV to RDF conversion with semantic type detection"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [java-time :as time]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.rdf.model ModelFactory ResourceFactory]
           [org.apache.jena.vocabulary RDF RDFS XSD]
           [org.apache.jena.datatypes.xsd XSDDatatype]
           [java.util UUID]))

(defn detect-semantic-type
  "Detect semantic type of a value using regex patterns"
  [value patterns]
  (when (and value (string? value) (not (str/blank? value)))
    (let [trimmed (str/trim value)]
      (->> patterns
           (keep (fn [[type pattern]]
                   (when (re-matches pattern trimmed)
                     type)))
           first))))

(defn infer-data-type
  "Infer XSD data type from string value"
  [value]
  (when (and value (not (str/blank? value)))
    (let [trimmed (str/trim value)]
      (cond
        (re-matches #"-?\d+" trimmed) XSDDatatype/XSDinteger
        (re-matches #"-?\d+\.\d+" trimmed) XSDDatatype/XSDdecimal
        (re-matches #"true|false|TRUE|FALSE" trimmed) XSDDatatype/XSDboolean
        (re-matches #"\d{4}-\d{2}-\d{2}" trimmed) XSDDatatype/XSDdate
        (re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" trimmed) XSDDatatype/XSDdateTime
        (re-matches #"\d{2}:\d{2}:\d{2}" trimmed) XSDDatatype/XSDtime
        :else XSDDatatype/XSDstring))))

(defn analyze-column
  "Analyze a column to determine its semantic and data types"
  [column-values config]
  (let [non-null-values (remove str/blank? column-values)
        sample-size (min (count non-null-values) 
                        (get-in config [:csv :sample-size] 1000))
        sample-values (take sample-size non-null-values)
        patterns (config/semantic-type-patterns config)]
    
    (when (seq sample-values)
      (let [;; Detect semantic types
            semantic-detections (frequencies 
                                (keep #(detect-semantic-type % patterns) 
                                      sample-values))
            
            ;; Detect data types
            data-type-detections (frequencies 
                                 (map infer-data-type sample-values))
            
            ;; Calculate confidence scores
            total-samples (count sample-values)
            semantic-confidence (when (seq semantic-detections)
                                 (/ (apply max (vals semantic-detections)) 
                                    total-samples))
            
            ;; Determine primary types
            primary-semantic-type (when (and semantic-confidence
                                           (>= semantic-confidence 
                                               (config/confidence-threshold config)))
                                   (key (apply max-key val semantic-detections)))
            
            primary-data-type (key (apply max-key val data-type-detections))]
        
        {:semantic-type primary-semantic-type
         :semantic-confidence semantic-confidence
         :data-type primary-data-type
         :data-type-confidence (/ (get data-type-detections primary-data-type 0) 
                                 total-samples)
         :sample-count total-samples
         :null-count (- (count column-values) (count non-null-values))
         :unique-count (count (distinct non-null-values))
         :semantic-detections semantic-detections
         :data-type-detections data-type-detections}))))

(defn create-property-uri
  "Create a property URI for a column"
  [base-uri table-name column-name]
  (str base-uri table-name "/" (str/replace column-name #"[^a-zA-Z0-9_]" "_")))

(defn create-resource-uri
  "Create a resource URI for a row"
  [base-uri table-name row-index]
  (str base-uri table-name "/row_" row-index))

(defn add-column-metadata
  "Add metadata about columns to the RDF model"
  [model base-uri table-name headers column-analyses]
  (doseq [[idx header] (map-indexed vector headers)]
    (let [analysis (nth column-analyses idx)
          property-uri (create-property-uri base-uri table-name header)
          property (.createProperty model property-uri)]
      
      ;; Add basic property metadata
      (.addProperty property RDF/type RDF/Property)
      (.addProperty property RDFS/label header)
      (.addProperty property RDFS/comment 
                   (str "Column " (inc idx) " from table " table-name))
      
      ;; Add semantic type if detected
      (when-let [semantic-type (:semantic-type analysis)]
        (.addProperty property 
                     (.createProperty model (str base-uri "hasSemanticType"))
                     (str semantic-type)))
      
      ;; Add confidence scores
      (when-let [confidence (:semantic-confidence analysis)]
        (.addProperty property 
                     (.createProperty model (str base-uri "semanticConfidence"))
                     (.createTypedLiteral model (float confidence))))
      
      ;; Add cardinality information
      (.addProperty property 
                   (.createProperty model (str base-uri "uniqueValues"))
                   (.createTypedLiteral model (:unique-count analysis)))
      
      (.addProperty property 
                   (.createProperty model (str base-uri "nullValues"))
                   (.createTypedLiteral model (:null-count analysis))))))

(defn add-row-data
  "Add row data to the RDF model"
  [model base-uri table-name headers column-analyses data]
  (doseq [[row-idx row] (map-indexed vector data)]
    (let [resource-uri (create-resource-uri base-uri table-name row-idx)
          resource (.createResource model resource-uri)]
      
      ;; Add type information
      (.addProperty resource RDF/type 
                   (.createResource model (str base-uri table-name)))
      
      ;; Add row index
      (.addProperty resource 
                   (.createProperty model (str base-uri "rowIndex"))
                   (.createTypedLiteral model row-idx))
      
      ;; Add column values
      (doseq [[col-idx value] (map-indexed vector row)]
        (when (and value (not (str/blank? value)))
          (let [header (nth headers col-idx)
                analysis (nth column-analyses col-idx)
                property-uri (create-property-uri base-uri table-name header)
                property (.createProperty model property-uri)
                data-type (:data-type analysis XSDDatatype/XSDstring)]
            
            (.addProperty resource property 
                         (.createTypedLiteral model (str/trim value) data-type))))))))

(defn detect-relationships
  "Detect potential relationships between columns"
  [headers column-analyses data]
  (let [indexed-data (map-indexed vector data)
        relationships (atom [])]
    
    ;; Look for foreign key relationships (columns with similar value distributions)
    (doseq [i (range (count headers))
            j (range (inc i) (count headers))]
      (let [col-i-values (set (map #(nth % i) data))
            col-j-values (set (map #(nth % j) data))
            intersection (clojure.set/intersection col-i-values col-j-values)
            jaccard-similarity (when (pos? (count (clojure.set/union col-i-values col-j-values)))
                                (/ (count intersection)
                                   (count (clojure.set/union col-i-values col-j-values))))]
        
        ;; If high overlap, potential relationship
        (when (and jaccard-similarity (> jaccard-similarity 0.7))
          (swap! relationships conj
                 {:type :potential-foreign-key
                  :from-column (nth headers i)
                  :to-column (nth headers j)
                  :similarity jaccard-similarity
                  :shared-values (count intersection)}))))
    
    ;; Look for hierarchical relationships (subset relationships)
    (doseq [i (range (count headers))
            j (range (count headers))
            :when (not= i j)]
      (let [col-i-values (set (map #(nth % i) data))
            col-j-values (set (map #(nth % j) data))]
        
        (when (clojure.set/subset? col-i-values col-j-values)
          (swap! relationships conj
                 {:type :hierarchical
                  :child-column (nth headers i)
                  :parent-column (nth headers j)
                  :coverage (/ (count col-i-values) (count col-j-values))}))))
    
    @relationships))

(defn csv->rdf
  "Convert CSV file to RDF model with semantic type detection"
  [csv-file config]
  (log/info "Converting CSV to RDF:" csv-file)
  
  (with-open [reader (io/reader csv-file)]
    (let [csv-data (csv/read-csv reader :separator (get-in config [:csv :delimiter] \,))
          headers (first csv-data)
          data (rest csv-data)
          table-name (-> csv-file io/file .getName (str/replace #"\.csv$" ""))
          base-uri (config/get-namespace-uri config "data")
          
          ;; Analyze each column
          transposed-data (apply map vector data)
          column-analyses (mapv #(analyze-column % config) transposed-data)
          
          ;; Create RDF model
          model (ModelFactory/createDefaultModel)]
      
      (log/info "Analyzed" (count headers) "columns with" (count data) "rows")
      
      ;; Set namespace prefixes
      (.setNsPrefix model "base" base-uri)
      (.setNsPrefix model "rdf" (RDF/getURI))
      (.setNsPrefix model "rdfs" (RDFS/getURI))
      (.setNsPrefix model "xsd" (XSD/getURI))
      
      ;; Add column metadata
      (add-column-metadata model base-uri table-name headers column-analyses)
      
      ;; Add row data
      (add-row-data model base-uri table-name headers column-analyses data)
      
      ;; Detect and add relationships
      (let [relationships (detect-relationships headers column-analyses data)]
        (log/info "Detected" (count relationships) "potential relationships")
        
        (doseq [rel relationships]
          (let [rel-uri (str base-uri "relationship/" (str (UUID/randomUUID)))
                rel-resource (.createResource model rel-uri)]
            (.addProperty rel-resource RDF/type 
                         (.createProperty model (str base-uri "Relationship")))
            (.addProperty rel-resource 
                         (.createProperty model (str base-uri "relationshipType"))
                         (str (:type rel)))
            
            (case (:type rel)
              :potential-foreign-key
              (do (.addProperty rel-resource 
                               (.createProperty model (str base-uri "fromColumn"))
                               (:from-column rel))
                  (.addProperty rel-resource 
                               (.createProperty model (str base-uri "toColumn"))
                               (:to-column rel))
                  (.addProperty rel-resource 
                               (.createProperty model (str base-uri "similarity"))
                               (.createTypedLiteral model (float (:similarity rel)))))
              
              :hierarchical
              (do (.addProperty rel-resource 
                               (.createProperty model (str base-uri "childColumn"))
                               (:child-column rel))
                  (.addProperty rel-resource 
                               (.createProperty model (str base-uri "parentColumn"))
                               (:parent-column rel))
                  (.addProperty rel-resource 
                               (.createProperty model (str base-uri "coverage"))
                               (.createTypedLiteral model (float (:coverage rel)))))))))
      
      (log/info "RDF model created with" (.size model) "triples")
      model)))