#!/usr/bin/env bash
set -e


function stage_release_local
{
	is_release_mode || return 0

	# At least for now, we override the default by removing PyPI publishing, since, for the time being,
	# we link this project through its github URL.

	printf "== TODO: Skipping build+PyPI publishing for now, fix it when necessary\n"
	poetry $CI_POETRY_DEFAULT_ARGS build

	printf "== Moving the project to the next version\n"
	poetry  $CI_POETRY_DEFAULT_ARGS version ${CI_NEW_SNAPSHOT_VER}

	# Mark what we have just done with the release tag
	release_commit_and_tag
	
	# And commit it
	release_commit_new_snapshot
}


printf "== Installing ci-build scripts and then running the build\n"
ci_build_url_base="https://raw.githubusercontent.com/Rothamsted/knetminer-common/refs/heads/ci-build-v2"
script_url="$ci_build_url_base/ci-build-v2/install.sh"
. <(curl --fail-with-body -o - "$script_url") "$ci_build_url_base" python-poetry

main
