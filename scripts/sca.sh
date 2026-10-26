#!/usr/bin/env bash
#
# Software-composition analysis: builds the jar, writes SBOMs, and scans them for known vulnerabilities.
#
#   Syft  - reads the built jar (what is really shipped) and writes a CycloneDX SBOM
#   Grype - scans that SBOM, and the SBOM Maven generated from the dependency graph, against the
#           vulnerability database; fails on High or above unless listed in .grype.yaml
#
# Both tools run as pinned Docker images, so nothing is installed on the host. Needs Docker, Java 21 and Maven.
# The vulnerability database changes daily, so this can turn red without any code change. That is why it is a
# separate gate (scheduled in CI) and not part of `mvn verify`, which must stay deterministic.
#
# Usage: scripts/sca.sh
set -euo pipefail

SYFT_IMAGE="anchore/syft:v1.55.0"
GRYPE_IMAGE="anchore/grype:v0.121.0"

cd "$(dirname "$0")/.."
OUT="target/sca"
CACHE="${GRYPE_DB_CACHE:-$HOME/.cache/dlq-healing-agent/grype}"
JAR="target/dlq-healing-agent-0.1.0-SNAPSHOT.jar"

command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }

# Always rebuild from clean: a stale SBOM left in target/ once reported an old library version.
mvn -B -q clean package -DskipTests
mkdir -p "$OUT" "$CACHE"

echo "== SBOM from the built jar (Syft)"
docker run --rm -v "$PWD/target:/in:ro" -v "$PWD/$OUT:/out" "$SYFT_IMAGE" \
  "/in/$(basename "$JAR")" -o cyclonedx-json=/out/syft.cdx.json -q
cp target/classes/META-INF/sbom/application.cdx.json "$OUT/maven.cdx.json"
echo "   $OUT/syft.cdx.json  and  $OUT/maven.cdx.json"

status=0
for sbom in syft.cdx.json maven.cdx.json; do
  echo "== Vulnerability scan of $sbom (Grype, fails on High or above)"
  docker run --rm -v "$PWD/$OUT:/in:ro" -v "$PWD/.grype.yaml:/config/.grype.yaml:ro" -v "$CACHE:/cache" \
    -e GRYPE_DB_CACHE_DIR=/cache "$GRYPE_IMAGE" "sbom:/in/$sbom" -c /config/.grype.yaml -q || status=$?
done

if [ "$status" -eq 0 ]; then echo "== SCA passed"; else echo "== SCA FAILED (exit $status)" >&2; fi
exit "$status"
