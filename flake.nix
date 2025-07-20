{
  description = "PG Semantic Schema - Clojure development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Java version compatible with Apache Jena
        jdk = pkgs.openjdk17;
        
        # Clojure CLI tools
        clojure = pkgs.clojure.override { jdk = jdk; };
        
        # Leiningen with the same JDK
        leiningen = pkgs.leiningen.override { jdk = jdk; };
        
        # PostgreSQL for testing
        postgresql = pkgs.postgresql_15;
        
        # Development dependencies
        devDeps = with pkgs; [
          # Core Clojure tooling
          jdk
          clojure
          leiningen
          
          # Database tools
          postgresql
          
          # Development utilities
          git
          curl
          wget
          
          # Text processing
          ripgrep
          fd
          
          # Build tools
          gnumake
          
          # Documentation tools
          pandoc
          
          # Optional: Clojure LSP for editor support
          clojure-lsp
          
          # Optional: Babashka for scripting
          babashka
        ];
        
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = devDeps;
          
          shellHook = ''
            echo "🚀 PG Semantic Schema Development Environment"
            echo "Java version: $(java -version 2>&1 | head -n 1)"
            echo "Clojure version: $(clojure -M --eval '(println (clojure-version))' 2>/dev/null || echo 'Not available')"
            echo "Leiningen version: $(lein version 2>/dev/null | head -n 1 || echo 'Not available')"
            echo ""
            echo "📋 Available commands:"
            echo "  lein repl        - Start Clojure REPL"
            echo "  lein run         - Run the semantic pipeline"
            echo "  lein test        - Run tests"
            echo "  lein deps        - Download dependencies"
            echo "  psql             - PostgreSQL client"
            echo "  clojure-lsp      - Language server for editor support"
            echo ""
            echo "📁 Project structure:"
            echo "  src/             - Source code"
            echo "  resources/       - Sample data and resources"
            echo "  dev/             - Development utilities"
            echo ""
            echo "🔧 To get started:"
            echo "  1. Run 'lein deps' to download dependencies"
            echo "  2. Run 'lein repl' to start development REPL"
            echo "  3. Try '(dev-help)' in the REPL for development functions"
            echo ""
            
            # Set Java home for consistency
            export JAVA_HOME="${jdk}/lib/openjdk"
            
            # Optimize JVM for development
            export JVM_OPTS="-Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
            
            # Set Leiningen home to local directory
            export LEIN_HOME="$PWD/.lein"
            mkdir -p "$LEIN_HOME"
            
            # PostgreSQL setup for local development
            export PGDATA="$PWD/.postgres"
            export PGHOST="localhost"
            export PGPORT="5432"
            export PGUSER="$USER"
            
            # Create PostgreSQL data directory if it doesn't exist
            if [ ! -d "$PGDATA" ]; then
              echo "📊 Setting up local PostgreSQL database..."
              initdb -D "$PGDATA" --auth-local=trust --auth-host=trust
              echo "port = 5432" >> "$PGDATA/postgresql.conf"
              echo "unix_socket_directories = '/tmp'" >> "$PGDATA/postgresql.conf"
            fi
            
            # Function to start PostgreSQL
            start_postgres() {
              if ! pg_ctl status -D "$PGDATA" > /dev/null 2>&1; then
                echo "🚀 Starting PostgreSQL..."
                pg_ctl start -D "$PGDATA" -l "$PGDATA/postgres.log"
                sleep 2
                createdb semantic_test 2>/dev/null || true
                echo "✅ PostgreSQL started (database: semantic_test)"
              else
                echo "✅ PostgreSQL already running"
              fi
            }
            
            # Function to stop PostgreSQL
            stop_postgres() {
              if pg_ctl status -D "$PGDATA" > /dev/null 2>&1; then
                echo "🛑 Stopping PostgreSQL..."
                pg_ctl stop -D "$PGDATA"
                echo "✅ PostgreSQL stopped"
              else
                echo "ℹ️  PostgreSQL not running"
              fi
            }
            
            # Export functions for use in shell
            export -f start_postgres
            export -f stop_postgres
            
            echo "🗄️  PostgreSQL functions available:"
            echo "  start_postgres   - Start local PostgreSQL server"
            echo "  stop_postgres    - Stop local PostgreSQL server"
            echo ""
          '';
          
          # Environment variables
          JAVA_HOME = "${jdk}/lib/openjdk";
          CLOJURE_HOME = "${clojure}";
          LEIN_HOME = ".lein";
          
          # JVM options for development
          JVM_OPTS = "-Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200";
          
          # Ensure proper locale
          LC_ALL = "en_US.UTF-8";
          LANG = "en_US.UTF-8";
        };
        
        # Additional outputs for CI/CD
        packages.default = pkgs.stdenv.mkDerivation {
          pname = "pg-semantic-schema";
          version = "0.1.0";
          
          src = ./.;
          
          buildInputs = [ jdk leiningen ];
          
          buildPhase = ''
            export LEIN_HOME=$(mktemp -d)
            lein uberjar
          '';
          
          installPhase = ''
            mkdir -p $out/bin $out/lib
            cp target/uberjar/pg-semantic-schema-*-standalone.jar $out/lib/
            
            cat > $out/bin/pg-semantic-schema << EOF
            #!/bin/sh
            exec ${jdk}/bin/java -jar $out/lib/pg-semantic-schema-*-standalone.jar "\$@"
            EOF
            
            chmod +x $out/bin/pg-semantic-schema
          '';
        };
        
        # Development scripts
        apps = {
          repl = flake-utils.lib.mkApp {
            drv = pkgs.writeShellScriptBin "repl" ''
              export JAVA_HOME="${jdk}/lib/openjdk"
              exec ${leiningen}/bin/lein repl
            '';
          };
          
          test = flake-utils.lib.mkApp {
            drv = pkgs.writeShellScriptBin "test" ''
              export JAVA_HOME="${jdk}/lib/openjdk"
              exec ${leiningen}/bin/lein test
            '';
          };
          
          analyze = flake-utils.lib.mkApp {
            drv = pkgs.writeShellScriptBin "analyze" ''
              export JAVA_HOME="${jdk}/lib/openjdk"
              if [ $# -lt 2 ]; then
                echo "Usage: nix run .#analyze <input.csv> <output.sql>"
                exit 1
              fi
              exec ${leiningen}/bin/lein run "$1" "$2"
            '';
          };
        };
      });
}