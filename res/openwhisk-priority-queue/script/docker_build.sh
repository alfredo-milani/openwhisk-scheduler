#!/usr/bin/env bash
# init environment (note: this line must be at the top script because bash expand aliases when reading lines)
source "$(dirname "${BASH_SOURCE}")/env/load.sh" --check-dep --expand-aliases


declare -r PROJ_ROOT="$(dirname "${BASH_SOURCE}")/../../../../openwhisk-priority-queue"
declare -r VERSION="$(cat "$(dirname "${BASH_SOURCE}")/__version__" | tr -d " \t\n\r")"

declare -r JAVA_HOME='/Library/Java/JavaVirtualMachines/jdk8u275.jdk'
declare -r JAVA_JRE="${JAVA_HOME}/jre"


usage() {
    cat <<EOF
Create docker image for Apache OpenWhisk.
EOF
}

docker_build() {
    cd "${PROJ_ROOT}"

    _info "Using Java at ${JAVA_HOME} and JRE at ${JAVA_JRE}."

    _info "Using Gradle to build project."
    ./gradlew distDocker || return 1

    _info "Using tag version - whisk/controller:${VERSION}."
    docker tag whisk/controller whisk/controller:"${VERSION}" || return 1
}

parse_input() {
    while [[ ${#} -gt 0 ]]; do
        case "${1}" in
            * )
                _error "Unknown command \"${1}\"."
                return 1
                ;;
        esac
    done
}

main() {

    parse_input "${@}" || return 1

    docker_build

}

main "${@}"