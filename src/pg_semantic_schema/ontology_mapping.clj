(ns pg-semantic-schema.ontology-mapping
  "Domain detection using standard ontologies and vocabularies"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [pg-semantic-schema.config :as config])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.vocabulary RDF RDFS]
           [org.apache.jena.query QueryFactory QueryExecutionFactory]))

;; Standard vocabulary URIs
(def vocabularies
  {:schema-org "https://schema.org/"
   :good-relations "http://purl.org/goodrelations/v1#"
   :foaf "http://xmlns.com/foaf/0.1/"
   :dublin-core "http://purl.org/dc/terms/"
   :fibo "https://spec.edmcouncil.org/fibo/"
   :hl7-fhir "http://hl7.org/fhir/"
   :dcat "http://www.w3.org/ns/dcat#"})

;; Schema.org business entities mapping
(def schema-org-mappings
  {:sales {:concepts ["Order" "Invoice" "PaymentMethod" "Product" "Offer" "PriceSpecification"]
           :properties ["orderNumber" "orderDate" "totalPrice" "discount" "quantity" "price"]}
   
   :finance {:concepts ["MonetaryAmount" "PaymentMethod" "Invoice" "BankAccount" "CreditCard"]
             :properties ["amount" "currency" "accountNumber" "balance" "interestRate"]}
   
   :customer {:concepts ["Person" "Customer" "ContactPoint" "PostalAddress" "Organization"]
              :properties ["email" "telephone" "address" "name" "contactType"]}
   
   :product {:concepts ["Product" "Brand" "Category" "Offer" "Review"]
             :properties ["sku" "gtin" "brand" "category" "description" "model"]}
   
   :organization {:concepts ["Organization" "Corporation" "LocalBusiness" "EmployeeRole"]
                  :properties ["employee" "department" "jobTitle" "worksFor" "salary" "manager" "hire" "position" "ssn" "commission"]}
   
   :location {:concepts ["Place" "PostalAddress" "GeoCoordinates" "Country"]
              :properties ["address" "latitude" "longitude" "postalCode" "addressCountry"]}
   
   :time {:concepts ["DateTime" "Date" "Time" "Duration"]
          :properties ["startDate" "endDate" "dateCreated" "dateModified"]}
   
   :energy {:concepts ["EnergyConsumption" "Meter" "Utility" "PowerSource"]
            :properties ["consumption" "meter" "energy" "power" "kwh" "demand" "tariff" "grid" "solar" "wind"]}
   
   :food {:concepts ["FoodEstablishment" "Restaurant" "Inspection" "FoodSafety"]
          :properties ["inspection" "establishment" "food" "restaurant" "license" "violation" "safety" "kitchen"]}
   
   :healthcare {:concepts ["Patient" "MedicalRecord" "Treatment" "Diagnosis" "Hospital"]
                :properties ["patient" "medical" "diagnosis" "treatment" "medication" "hospital" "physician" "insurance"]}
   
   :transportation {:concepts ["Shipment" "Logistics" "Carrier" "Delivery"]
                    :properties ["shipment" "tracking" "delivery" "carrier" "logistics" "freight" "warehouse" "route"]}
   
   :manufacturing {:concepts ["Production" "QualityControl" "Inspection" "Manufacturing"]
                   :properties ["production" "quality" "inspection" "batch" "manufacturing" "defect" "tolerance" "standard"]}})

;; GoodRelations e-commerce ontology mappings
(def good-relations-mappings
  {:sales {:concepts ["Offering" "BusinessEntity" "PaymentMethod" "DeliveryMethod"]
           :properties ["hasBusinessFunction" "hasPriceSpecification" "validThrough"]}
   
   :product {:concepts ["ProductOrService" "Brand" "Manufacturer"]
             :properties ["hasManufacturer" "hasBrand" "hasEAN_UCC-13"]}})

;; FOAF social/organizational mappings
(def foaf-mappings
  {:person {:concepts ["Person" "Agent" "Group" "Organization"]
            :properties ["name" "mbox" "phone" "homepage" "workplaceHomepage"]}
   
   :organization {:concepts ["Organization" "Group"]
                  :properties ["member" "fundedBy" "homepage"]}})

;; Healthcare HL7 FHIR mappings (simplified)
(def fhir-mappings
  {:healthcare {:concepts ["Patient" "Practitioner" "Encounter" "Medication" "Observation"]
                :properties ["identifier" "active" "name" "telecom" "address" "birthDate"]}})

(defn load-ontology-file
  "Load ontology from local file or URL"
  [ontology-uri]
  (try
    (let [model (ModelFactory/createDefaultModel)]
      ;; In practice, you'd load from actual ontology files
      ;; For now, we'll use our mappings as a starting point
      (log/info "Loading ontology from" ontology-uri)
      model)
    (catch Exception e
      (log/warn "Could not load ontology" ontology-uri ":" (.getMessage e))
      nil)))

(defn create-concept-similarity-map
  "Create a map for fuzzy concept matching"
  []
  {:order ["order" "purchase" "sale" "transaction" "invoice" "receipt"]
   :customer ["customer" "client" "buyer" "purchaser" "consumer" "user"]
   :product ["product" "item" "good" "merchandise" "sku" "article"]
   :price ["price" "cost" "amount" "value" "fee" "charge" "rate"]
   :quantity ["quantity" "amount" "count" "number" "volume" "units"]
   :person ["person" "individual" "user" "employee" "staff" "worker"]
   :organization ["organization" "company" "business" "corporation" "firm"]
   :location ["location" "address" "place" "site" "venue" "facility"]
   :time ["date" "time" "timestamp" "when" "period" "duration"]
   :contact ["email" "phone" "telephone" "contact" "communication"]})

(defn normalize-term
  "Normalize a term for matching (remove prefixes, suffixes, etc.)"
  [term]
  (-> term
      str/lower-case
      (str/replace #"_id$|_key$|_date$|_time$|_at$|_on$" "")
      (str/replace #"^is_|^has_|^get_|^set_" "")
      (str/replace #"_+" "_")
      (str/replace #"^_|_$" "")))

(defn match-concept-to-ontology
  "Match a column concept to ontology concepts"
  [column-term concept-map ontology-mappings]
  (let [normalized-term (normalize-term column-term)
        similarity-map (create-concept-similarity-map)]
    
    ;; Find best matching concept
    (reduce (fn [best-match [concept-key similar-terms]]
              (if (some #(str/includes? normalized-term %) similar-terms)
                (let [score (count (filter #(str/includes? normalized-term %) similar-terms))]
                  (if (> score (:score best-match 0))
                    {:concept concept-key :score score}
                    best-match))
                best-match))
            {:concept nil :score 0}
            similarity-map)))

(defn analyze-domain-with-ontologies
  "Analyze domain using multiple ontologies"
  [column-roles semantic-types]
  (let [column-names (map #(-> (:property %) (str/split #"/") last) column-roles)
        semantic-type-set (set (map :semanticType semantic-types))
        
        ;; Match columns to ontology concepts
        schema-org-matches (reduce (fn [acc [domain mapping]]
                                    (let [concept-matches (count (filter (fn [col-name]
                                                                          (some #(str/includes? (str/lower-case col-name) %)
                                                                               (:properties mapping)))
                                                                        column-names))]
                                      (assoc acc domain concept-matches)))
                                  {} schema-org-mappings)
        
        ;; Weight semantic types
        semantic-weights {:email 2 :phone 2 :currency 3 :date 1 :ssn 2}
        semantic-boost (reduce (fn [acc semantic-type]
                                (+ acc (get semantic-weights (keyword semantic-type) 0)))
                              0 semantic-type-set)
        
        ;; Calculate domain scores
        domain-scores (reduce (fn [scores [domain score]]
                               (let [boosted-score (+ score
                                                     (if (contains? #{:customer :sales :finance} domain)
                                                       semantic-boost 0))]
                                 (assoc scores domain boosted-score)))
                             {} schema-org-matches)
        
        ;; Find best domain
        best-domain (if (empty? domain-scores)
                     :general
                     (key (apply max-key val domain-scores)))
        best-score (get domain-scores best-domain 0)]
    
    (log/info "Domain analysis with ontologies:")
    (log/info "  Schema.org matches:" schema-org-matches)
    (log/info "  Semantic boost:" semantic-boost)
    (log/info "  Best domain:" best-domain "with score:" best-score)
    
    {:detected-domain best-domain
     :confidence (/ best-score (max 1 (count column-names)))
     :ontology-matches {:schema-org schema-org-matches}
     :semantic-indicators semantic-type-set}))

(defn suggest-schema-namespace
  "Suggest schema namespace based on detected domain and ontologies"
  [domain-analysis]
  (let [domain (:detected-domain domain-analysis)
        confidence (:confidence domain-analysis)]
    
    (if (> confidence 0.3)
      (case domain
        :sales (str (vocabularies :schema-org) "schema/sales")
        :finance (str (vocabularies :fibo) "schema/financial")
        :customer (str (vocabularies :schema-org) "schema/customer")
        :product (str (vocabularies :good-relations) "schema/product")
        :organization (str (vocabularies :foaf) "schema/organization")
        :healthcare (str (vocabularies :hl7-fhir) "schema/patient")
        :location (str (vocabularies :schema-org) "schema/place")
        (str (vocabularies :schema-org) "schema/general"))
      (str (vocabularies :schema-org) "schema/data"))))

(defn generate-rdf-annotations
  "Generate RDF annotations linking to standard ontologies"
  [column-roles detected-domain]
  (mapv (fn [col]
          (let [column-name (-> (:property col) (str/split #"/") last str/lower-case)
                semantic-type (:semantic-type col)
                
                ;; Map to Schema.org properties
                schema-org-property (cond
                                     (str/includes? column-name "email") "email"
                                     (str/includes? column-name "phone") "telephone"
                                     (str/includes? column-name "name") "name"
                                     (str/includes? column-name "address") "address"
                                     (str/includes? column-name "price") "price"
                                     (str/includes? column-name "quantity") "quantity"
                                     (str/includes? column-name "date") "dateCreated"
                                     (= semantic-type "currency") "priceSpecification"
                                     :else nil)
                
                ontology-uri (when schema-org-property
                              (str (vocabularies :schema-org) schema-org-property))]
            
            (cond-> col
              ontology-uri (assoc :ontology-mapping ontology-uri)
              ontology-uri (assoc :ontology-source "Schema.org"))))
        column-roles))

(defn create-ontology-aware-config
  "Create configuration that includes ontology mappings"
  [base-config domain-analysis]
  (assoc base-config
         :ontology {:detected-domain (:detected-domain domain-analysis)
                   :confidence (:confidence domain-analysis)
                   :schema-namespace (suggest-schema-namespace domain-analysis)
                   :vocabularies-used #{:schema-org :good-relations :foaf}}))

(comment
  ;; Example usage with real ontology files
  ;; You could download and use actual ontology files:
  
  ;; Schema.org in JSON-LD format
  ;; wget https://schema.org/version/latest/schemaorg-current-https.jsonld
  
  ;; GoodRelations RDF/XML
  ;; wget http://purl.org/goodrelations/v1.owl
  
  ;; FOAF RDF/XML  
  ;; wget http://xmlns.com/foaf/spec/index.rdf
  
  ;; Then load them with Jena:
  ;; (def schema-org-model (ModelFactory/createDefaultModel))
  ;; (.read schema-org-model "schemaorg-current-https.jsonld" "JSON-LD")
  )