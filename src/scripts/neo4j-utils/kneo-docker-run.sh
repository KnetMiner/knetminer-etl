#!/usr/bin/env bash
set -euo pipefail

neo_user="${NEO4J_USER:-neo4j}"
neo_pwd="${NEO4J_PASSWORD:-testTest}"
neo_version=""
volume_opt=""
neo_bolt_port=${NEO4J_BOLT_PORT:-7687}
neo_http_port=${NEO4J_HTTP_PORT:-7474}

function usage ()
{
	cat <<EOT


Usage: $0 [OPTIONS]

Runs Neo4j through its Docker image.

Options:
  --user <usr>         Neo4j username (default: "$neo_user", equivalent to the NEO4J_USER)
  --password <pwd>     Neo4j password (default: "$neo_pwd", equivalent to the NEO4J_PASSWORD)
  --version <version>  Neo4j version to use (default: empty/latest)
  --volume <path>      Local data path to mount as Docker volume (default: empty, no volume, no persistence)
	--bolt-port <port>   Bolt port to expose (default: $neo_bolt_port, equivalent to the NEO4J_BOLT_PORT)
	--http-port <port>   HTTP port to expose (default: $neo_http_port, equivalent to the NEO4J_HTTP_PORT)
	--help							 Show this help message and exit

EOT
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--user) neo_user="$2"; shift 2 ;;
		--password) neo_pwd="$2"; shift 2 ;;
		--version) neo_version=":$2"; shift 2 ;;
		--volume) volume_opt="-v $2:/data"; shift 2 ;;
		--bolt-port) neo_bolt_port="$2"; shift 2 ;;
		--http-port) neo_http_port="$2"; shift 2 ;;
		--help) usage; exit 2 ;;
		*) echo "Unknown option: $1" >&2; usage; exit 1 ;;
	esac
done

docker run \
	--env NEO4J_AUTH="$neo_user/$neo_pwd" \
	-p $neo_bolt_port:7687 -p $neo_http_port:7474 \
  $volume_opt \
	neo4j$neo_version &

docker_pid=$!

function cleanup {
	echo "Stopping Neo4j Docker container (PID $docker_pid)..."
	kill $docker_pid || true
	wait
	printf "\n\n|==== Neo4j server ended\n\n"
}
trap cleanup EXIT

wait
