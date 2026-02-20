#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

mvn -f old_migrator/pom.xml test
mvn -f old_migrator/pom.xml -DskipTests clean package
