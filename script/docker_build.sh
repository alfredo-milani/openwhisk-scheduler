#!/usr/bin/env bash
# init environment (note: this line must be at the top script because bash expand aliases when reading lines)
source "$(dirname "${BASH_SOURCE}")/env/load.sh" --check-dep --expand-aliases


declare -r PROJ_ROOT="$(dirname "${BASH_SOURCE}")/.."  # rendere path assoluto
declare -r RES_ROOT="${PROJ_ROOT}/res"
declare -r DOCKER_ROOT="${PROJ_ROOT}/docker"
declare -r TARGET_ROOT="${PROJ_ROOT}/target"
declare -r VERSION="$(cat "${DOCKER_ROOT}/__version__" | tr -d " \t\n\r")"

declare -r IMAGE_NAME='ow-scheduler'


usage() {
    cat <<EOF
Create docker image from Docker file contained in ${DOCKER_ROOT}.
EOF
}

docker_build() {
    _info "Building image version ${VERSION}."
    (
        # cd "${DOCKER_ROOT}"

        # create missing directories
        declare -r -a directories=("bin" "conf" "script")
        for directory in "${directories[@]}"; do
            if [[ ! -d "${DOCKER_ROOT}/${directory}" ]]; then
                mkdir -p "${DOCKER_ROOT}/${directory}"
                _info "Created missing directory \"${DOCKER_ROOT}/${directory}\"."
            fi
        done

        # make hard links to workaround Docker context limitation
        ln "${TARGET_ROOT}/openwhisk-scheduler-"*"jar-with-dependencies.jar" "${DOCKER_ROOT}/bin"
        ln "${PROJ_ROOT}/src/main/resources/configuration.properties" "${DOCKER_ROOT}/conf"

        # generate docker image
        docker build -t "${IMAGE_NAME}":"${VERSION}" "${DOCKER_ROOT}" || return 1

        # remove hard links
        rm "${DOCKER_ROOT}/bin/openwhisk-scheduler-"*"jar-with-dependencies.jar"
        rm "${DOCKER_ROOT}/conf/"*".properties"
    )
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