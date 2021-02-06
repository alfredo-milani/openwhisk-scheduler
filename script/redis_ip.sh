#!/usr/bin/env bash



# init environment (note: this line must be at the top script because bash expand aliases when reading lines)
source "$(dirname "${BASH_SOURCE}")/env/load.sh" --check-dep --expand-aliases


usage() {
    cat <<EOF
-s | --service
    Kubernetes namespace to obtain.
-g | --to-grep
    Kubernetes pod to grep.
EOF
}

redis_ip() {
    printf "$(kc-g-pi openwhisk | grep redis | awk '{print $6}')"
}

parse_input() {
    while [[ ${#} -gt 0 ]]; do
        case "${1}" in
            -g | --to-grep )
                shift
                if [[ -z "${1}" ]]; then
                    _error "Value for -g option can not be empty."
                    return 1
                fi
                TO_GREP="${1}"
                shift
                ;;

            help | HELP | -h | -H | --help | --HELP )
                usage
                return 0
                ;;

            -s | --service )
                shift
                if [[ -z "${1}" ]]; then
                    _error "Value for -s option can not be empty."
                    return 1
                fi
                SERVICE="${1}"
                shift
                ;;

            * )
                _error "Unknown command \"${1}\"."
                return 1
                ;;
        esac
    done
}

main() {

    local -r SERVICE='openwhisk'
    local -r TO_GREP='redis'

    parse_input "${@}" || return 1

    redis_ip

}

main "${@}"
