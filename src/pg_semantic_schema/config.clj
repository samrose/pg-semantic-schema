(ns pg-semantic-schema.config
  "Configuration and Jena setup utilities"
  (:require [taoensso.timbre :as log])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.reasoner ReasonerRegistry]
           [org.apache.jena.reasoner.rulesys GenericRuleReasoner]
           [org.apache.jena.vocabulary RDF RDFS OWL XSD]
           [org.apache.jena.sparql.vocabulary FOAF]
           [org.apache.jena.ontology OntModelSpec]))

(def ^:dynamic *default-config*
  {:jena {:reasoner-type :owl-micro
          :namespace-base "http://example.org/schema/"
          :model-spec OntModelSpec/OWL_MEM_MICRO_RULE_INF}
   :csv {:delimiter \,
         :quote \"
         :escape \\
         :sample-size 1000}
   :postgres {:schema-prefix "semantic_"
              :fact-table-suffix "_fact"
              :dim-table-suffix "_dim"
              :bridge-table-suffix "_bridge"}
   :semantic-types {:confidence-threshold 0.8
                    :patterns {:email #"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                               :phone #"(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}"
                               :currency #"\$?\d+\.?\d*"
                               :date #"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{2}-\d{2}-\d{4}"
                               :time #"\d{2}:\d{2}(:\d{2})?"
                               :url #"https?://[^\s/$.?#].[^\s]*"
                               :zip-code #"\d{5}(-\d{4})?"
                               :ssn #"\d{3}-\d{2}-\d{4}"}}})

(defn create-ontology-model
  "Create a Jena ontology model with reasoning capabilities"
  ([config] (create-ontology-model config nil))
  ([config base-uri]
   (let [model-spec (get-in config [:jena :model-spec] OntModelSpec/OWL_MEM_MICRO_RULE_INF)
         model (ModelFactory/createOntologyModel model-spec)]
     (when base-uri
       (.setNsPrefix model "base" base-uri))
     (.setNsPrefix model "rdf" (RDF/getURI))
     (.setNsPrefix model "rdfs" (RDFS/getURI))
     (.setNsPrefix model "owl" (OWL/getURI))
     (.setNsPrefix model "xsd" (XSD/getURI))
     (.setNsPrefix model "foaf" (FOAF/getURI))
     model)))

(defn create-reasoner
  "Create a Jena reasoner based on configuration"
  [config]
  (case (get-in config [:jena :reasoner-type] :owl-micro)
    :owl-micro (ReasonerRegistry/getOWLMicroReasoner)
    :owl-mini (ReasonerRegistry/getOWLMiniReasoner)
    :owl (ReasonerRegistry/getOWLReasoner)
    :rdfs (ReasonerRegistry/getRDFSReasoner)
    :transitive (ReasonerRegistry/getTransitiveReasoner)
    (ReasonerRegistry/getOWLMicroReasoner)))

(defn get-namespace-uri
  "Get namespace URI for a given prefix"
  [config prefix]
  (str (get-in config [:jena :namespace-base]) prefix "/"))

(defn semantic-type-patterns
  "Get semantic type detection patterns from config"
  [config]
  (get-in config [:semantic-types :patterns]))

(defn confidence-threshold
  "Get confidence threshold for semantic type detection"
  [config]
  (get-in config [:semantic-types :confidence-threshold] 0.8))