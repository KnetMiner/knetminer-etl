#!/usr/bin/env bash

set -eo pipefail

if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
	cat <<EOF


  Usage: $0 [-f|--force] [target-dir]

Links the content of dist-resources/ from the installed distribution to the target directory
(defaults to current working directory).

Options:
  -f, --force    Force linking by removing existing target paths if they exist.

EOF
	exit 0
fi

# Parse options
do_force=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      do_force=true
      shift
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      target_dir="$1"
      shift
      ;;
  esac
done

# Target directory defaults to current working directory.
target_dir="${target_dir:-$PWD}"

printf "\n\n|==== Deploying knetminer-etl resources to %s\n\n" "$target_dir"


mkdir -p "$target_dir"

python_bin="${PYTHON:-python}"

# Locate the dist-resources directory from the installed distribution
#
dist_resources_dir="$($python_bin - <<'PY'
from pathlib import PurePosixPath
import importlib.metadata as md

dist = None
for dist_name in ("knetminer-etl", "knetminer_etl"):
  try:
    dist = md.distribution(dist_name)
    break
  except md.PackageNotFoundError:
    continue

if dist is None:
  raise SystemExit(0)

candidate = dist.locate_file(PurePosixPath("dist-resources"))
if candidate.is_dir():
  print(candidate)
  raise SystemExit(0)

# Fallback: locate from files recorded for this exact distribution.
for rel in dist.files or ():
  if str(rel).startswith("dist-resources/"):
    path = dist.locate_file(rel)
    while path.name != "dist-resources" and path.parent != path:
      path = path.parent
    if path.name == "dist-resources" and path.is_dir():
      print(path)
      break
PY
)"

if [[ -z "${dist_resources_dir}" ]]; then
  echo "ERROR: Cannot find dist-resources in the current Python environment." >&2
  echo "Make sure knetminer-etl is installed in the active virtual environment." >&2
  exit 1
fi

# And then link them
#
for source in "$dist_resources_dir"/*; do
  [[ -e "$source" ]] || continue

  target_link="$target_dir/$(basename "$source")"

  printf "|== \"%s\" -> \"%s\"" "$source" "$target_link"

  if [[ -e "$target_link" || -L "$target_link" ]]; then
    printf " (already exists, "
    if ! $do_force; then
      printf "skipping)\n"
      continue
    fi
    printf "overriding)\n"
    rm -rf "$target_link"
  else
    printf "\n"
  fi

  ln -s "$source" "$target_link"
done

printf "|==== /done\n\n"
