#!/usr/bin/env bash



# init environment (note: this line must be at the top script because bash expand aliases when reading lines)
source "$(dirname "${BASH_SOURCE}")/env/load.sh" --check-dep --expand-aliases


usage() {
    cat <<EOF
-d /path | --destination /path
    Specify destination directory of the copy.
-s service_name | --service service_name
    Collect output from service_name container.
EOF
}

parse_input() {
    while [[ ${#} -gt 0 ]]; do
        case "${1}" in
            -d | --destination )
                shift
                [[ -n "${1}" ]] && DESTINATION="${1}"
                shift
                ;;

            help | HELP | -h | -H | --help | --HELP )
                usage
                exit 0
                ;;

            -s | --service )
                shift
                [[ -n "${1}" ]] && SERVICE="${1}"
                shift
                ;;

            * )
                _error "Unknown command \"${1}\"."
                return 1
                ;;
        esac
    done
}

validate_input() {
    if [[ ! -e "${DESTINATION}" || ! -d "${DESTINATION}" ]]; then
        _error "Destination *must* exists and be a valid directory (\"${DESTINATION}\" does not exists)."
        return 1
    elif [[ -z "${SERVICE}" ]]; then
        _error "Service *must* not be empty."
        return 1
    fi
}

write_output() {
    local -r docker_container="$(docker ps | grep "${SERVICE}")"
    if [[ -z "${docker_container}" ]]; then
        _error "No Docker container match specified service (${SERVICE})."
        return 1
    fi

    FILENAME="${SERVICE}.log"
    _info "Collecting output of container service ${SERVICE} in ${DESTINATION}/${FILENAME}."
    echo "${docker_container}" | xargs -n1 docker attach > "${DESTINATION}/${FILENAME}" || return 1
}

main() {

    local SERVICE=''
    local DESTINATION='.'
    local FILENAME="${SERVICE}.log"

    parse_input "${@}" || return 1

    validate_input || return 1

    write_output || return 1

}

main "${@}"