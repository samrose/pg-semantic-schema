(defproject pg-semantic-schema "0.1.0-SNAPSHOT"
  :description "Semantic data pipeline for converting CSV to PostgreSQL star/snowflake schemas using Apache Jena"
  :url "https://github.com/samrose/pg-semantic-schema"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 ;; Apache Jena dependencies for RDF processing and reasoning
                 [org.apache.jena/apache-jena-libs "4.10.0" :extension "pom" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.jena/jena-core "4.10.0"]
                 [org.apache.jena/jena-arq "4.10.0"]
                 [org.apache.jena/jena-tdb2 "4.10.0"]
                 [org.apache.jena/jena-fuseki-main "4.10.0"]
                 ;; CSV parsing
                 [org.clojure/data.csv "1.0.1"]
                 ;; JSON processing for metadata
                 [org.clojure/data.json "2.4.0"]
                 ;; String utilities for semantic type detection
                 [clojure.java-time "1.4.2"]
                 [com.taoensso/timbre "6.3.1"]
                 ;; Database connectivity for PostgreSQL
                 [org.postgresql/postgresql "42.7.1"]
                 [com.github.seancorfield/next.jdbc "1.3.909"]
                 ;; Development and testing
                 [org.clojure/test.check "1.1.1"]
                 [criterium "0.4.6"]]
  :main ^:skip-aot pg-semantic-schema.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :dev {:dependencies [[org.clojure/tools.namespace "1.4.4"]
                                  [org.clojure/tools.trace "0.7.11"]]
                   :source-paths ["dev"]
                   :jvm-opts ["-Xmx2g"]}}
  :repl-options {:init-ns pg-semantic-schema.core}
  :resource-paths ["resources"]
  :test-paths ["test"])