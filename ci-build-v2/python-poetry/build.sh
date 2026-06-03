#!/usr/bin/env bash
set -eE -o pipefail

# For the moment, we are publishing upon each commit, since we download distro files straight
# from github, even if they aren't released yet. TODO: to be changed later.
#
function stage_build_local
{
	stage_build
	poetry $CI_POETRY_DEFAULT_ARGS build

  # Are there actual changes to commit?
	[[ -z "$(git status --porcelain)" ]] && return 0
	
	git add dist
	git commit -m "build: upgrade the distro files on github (from CI script)"
	export CI_NEEDS_PUSH=true
}


# At least for now, we override the default by removing PyPI publishing, since, for the time being,
# we link this project through its github URL.
# 
# This will need to be changed in future, see stage_build_local() above.
#
function stage_release_local
{
	is_release_mode || return 0

	printf "== TODO: Skipping build+PyPI publishing for now, fix it when necessary\n"
	# TODO: see stage_build_local() for details on why this is not done here.
	# poetry $CI_POETRY_DEFAULT_ARGS build

	printf "== Moving the project to the next version\n"
	poetry  $CI_POETRY_DEFAULT_ARGS version ${CI_NEW_SNAPSHOT_VER}

	# Mark what we have just done with the release tag
	release_commit_and_tag
	
	# And commit it
	release_commit_new_snapshot
}


printf "== Installing ci-build scripts and then running the build\n"
ci_build_version='1.2'
ci_build_url_base="https://raw.githubusercontent.com/KnetMiner/knetminer-ci/refs/tags/$ci_build_version"
script_url="$ci_build_url_base/ci-build-v2/install.sh"
. <(curl --fail-with-body -o - "$script_url") "$ci_build_url_base" python-poetry

main
