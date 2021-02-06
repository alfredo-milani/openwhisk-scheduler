#!/usr/bin/env bash



# init environment (note: this line must be at the top script because bash expand aliases when reading lines)
source "$(dirname "${BASH_SOURCE}")/env/load.sh" --check-dep --expand-aliases


declare -r PROJ_ROOT="$(dirname "${BASH_SOURCE}")/.."
declare -r RES_ROOT="${PROJ_ROOT}/res"
declare -r FN_ROOT="${PROJ_ROOT}/src"


abort() {
    _info "Action interrupted by user."
}

register_signal() {
    trap 'abort' SIGINT SIGKILL SIGTERM SIGUSR1 SIGUSR2
}

usage() {
    local usage

    read -r -d '' usage << EOF
${BASH_SOURCE} command action [params]
    
COMMAND
    
    deploy
        cmp kind package/composition concurrency
            Create composition file, using compose command, and deploy it, using deploy command (openwhisk-composer).
            E.g. ${BASH_SOURCE} cmp deploy nodejs:12 image_processing/imgman_cmp 5
            Note: for now, the default kind for composition action used in this script is nodejs, so kind value must be specified bu will be ignored.
            Note: the specified concurrency level must be within the limit of the deployed OpenWhisk system.
        fn kind package/function concurrency
            Remove zip file, if any, create new zip file and deploy function contained in package.
            E.g. ${BASH_SOURCE} fn python:3 deploy image_processing/imgman 5
            Note: the specified concurrency level must be within the limit of the deployed OpenWhisk system.
    
    help | HELP | -h | -H | --help | --HELP
        Print this help message.
    ow
        dashboard
            Open Grafa dashboard on browser.
        forwarding
            Enabling port forwarding for Prometheus (in case Grafana does not show any data).
        install
            Install Apache OpenWhisk on Kubernetes cluster.
        uninstall
            Uninstall Apache OpenWhisk from Kubernetes cluster.
        upgrade
            Upgrade Apache OpenWhisk installation with new configuration.
        wa
            Watch Kubernetes pods on 'openwhisk' namespace.

    query
        rest function data
            Send blocking request to OpenWhisk function using command line utility like curl or wget, sending data.
            data file will be considered as path to filename.
            data field must be null string ('') if you not intend sending data within request.
        wsk function data
            Send blocking request to OpenWhisk function using command line tool wsk, sending data. 
            data file will be considered as path to filename.
            data field must be null string ('') if you not intend sending data within request.
EOF
    # wsk function data
    #     Send request to OpenWhisk function using command line tool wsk, sending data. 
    #     data file will be considered as path to filename (binary).
    # wskjson function data
    #     Send request to OpenWhisk function using command line tool wsk, sending data. 
    #     data file will be considered as path to json file.

    printf "${usage}\n"   
}

# Args:
# {1} [mnd]: action to perform (cmp or fn)
# {2} [mnd]: runtime kind to deploy, e.g. python:3 or nodejs:10
# {3} [mnd]: action to manage, in format "package_name/action", e.g. image_processing/resize
# {4} [mnd]: conccurency level, e.g. 5 or 10 or 20; the concurrency limit must be supported by the deployed system
parse_deploy_input() {
    local -r command="${1}"
    local -r kind="${2}"
    local -r action="${3}"
    local -r concurrency="${4}"
    if [[ -z "${command}" || -z "${kind}" || -z "${action}" || -z "${concurrency}" ]]; then
        _error "Missing first, second, third or fourth positional arguments required."
        return 1
    fi

    case "${command}" in
        cmp )
            if [[ ! -e "${FN_ROOT}/${action}" || ! -d "${FN_ROOT}/${action}" ]]; then
                _error "The function \"${FN_ROOT}/${action}\" specified does not exists."
                return 1
            elif [[ ! -e "${FN_ROOT}/${action}/node_modules" || ! -d "${FN_ROOT}/${action}/node_modules" || ! -r "${FN_ROOT}/${action}/node_modules" ]]; then
                _error "Missing node_modules directory."
                return 1
            fi

            (
                cd "${FN_ROOT}/${action}"

                declare -r package="$(dirname "${action}")"
                declare -r cmp="$(basename "${action}")"

                _info "Invoking compose command over \"${FN_ROOT}/${action}/${cmp}.js\"."
                npx compose "./${cmp}.js" > "./${cmp}.json"

                # for now, only consider to deploy nodejs composition kind
                _info "Invoking deploy command over \"${FN_ROOT}/${action}/${cmp}.json\", with concurrency level ${concurrency}."
                npx deploy "${cmp}" "./${cmp}.json" --insecure --overwrite --annotation raw-http=true
                # if specified concurrency level is greter then 1, update it
                if [[ "${concurrency}" -gt 1 ]]; then
                    wsk-act-ud "${cmp}" --concurrency "${concurrency}"
                fi

                # retrieving URL for specified action
                _info "Action reachable at \"$(wsk-act-url "${cmp}" | awk 'NR==2')\""
            )
            ;;

        fn )
            if [[ ! -e "${FN_ROOT}/${action}" || ! -d "${FN_ROOT}/${action}" ]]; then
                _error "The function \"${FN_ROOT}/${action}\" specified does not exists."
                return 1
            fi

            (
                cd "${FN_ROOT}/${action}"

                declare -r package="$(dirname "${action}")"
                declare -r fn="$(basename "${action}")"

                # remove old action archive
                if [[ -e "./${fn}.zip" ]]; then
                    _info "Removing old zip file \"${FN_ROOT}/${action}/${fn}.zip\"."
                    rm "./${fn}.zip"
                fi

                _info "Compressing files with zip -r -y"
                zip -y -u -r "${fn}.zip" "./"* 1> "${DEV_NULL}"

                # update action, as raw web action
                _info "Update/create OpenWhisk action named \"${fn}\", with concurrency level ${concurrency}."
                wsk-act-ud "${fn}" --kind "${kind}" "${fn}.zip" --web raw --concurrency "${concurrency}"

                # retrieving URL for specified action
                _info "Action reachable at \"$(wsk-act-url "${fn}" | awk 'NR==2')\""
            )
            ;;

        * )
            _error "Unknown option \"${1}\" for command deploy."
            return 1
            ;;
    esac
}

# Args:
# {1} [mnd]: action to perform
parse_ow_input() {
    if [[ -z "${1}" ]]; then
        _error "Missing first positional argument requires."
        return 1
    fi

    case "${1}" in
        dashboard )
            _info "Opening Grafana dashboard at \"https://localhost:31001/monitoring/dashboards\"."
            open https://localhost:31001/monitoring/dashboards
            ;;

        forwarding )
            _info "Enabling port forwarding for Prometheus (in case Grafana does not show any data)."
            kc port-forward svc/owdev-prometheus-server 9090:9090 --namespace openwhisk
            ;;

        install )
            _info "Labeling Kubernetes nodes as invoker."
            kc label nodes --all openwhisk-role=invoker

            _info "Installing openwhisk application using Helm."
            # helm install owdev "${RES_ROOT}/openwhisk-deploy-kube/helm/openwhisk" \
            #     -n openwhisk --create-namespace -f "${RES_ROOT}/openwhisk-deploy-kube/cluster/my_cluster_pq.yaml"

            _info "Metrics not enables."
            helm install owdev "${RES_ROOT}/openwhisk-deploy-kube/helm/openwhisk" \
                -n openwhisk --create-namespace -f "${RES_ROOT}/openwhisk-deploy-kube/cluster/my_cluster_pq_no_metrics.yaml"
            ;;

        uninstall )
            _info "Uninstalling openwhisk application using Helm."
            helm uninstall owdev -n openwhisk
            ;;

        upgrade )
            _info "Upgrading only controller pods."
            helm upgrade owdev "${RES_ROOT}/openwhisk-deploy-kube/helm/openwhisk" \
                -n openwhisk -f "${RES_ROOT}/openwhisk-deploy-kube/cluster/my_cluster_pq_no_metrics.yaml"
            ;;

        wa )
            _info "Watching Kubernetes pods on 'openwhisk' namespace."
            wa kc-g-pi openwhisk
            ;;

        * )
            _error "Unknown option \"${1}\" for command ow."
            return 1
            ;;
    esac
}

# Args:
# {1} [mnd]: action to perform
# {2} [mnd]: openwhisk function to invoke
# {3} [mnd]: data to send when invoking openwhisk function (if there is no data to send, pass empty string '')
parse_query_input() {
    if [[ -z "${1}" || -z "${2}" ]]; then
        _error "Missing first, second arguments required."
        return 1
    fi

    case "${1}" in
        rest )
            # get action withot extension, if any
            local -r service=(${2//./ })
            local -r action="${service[0]}"
            local -r ext="${service[1]}"

            # retrieving URL for specified action
            # output of 'wsk-act-url action' is like:
            # "ok: got action resize
            # https://localhost:31001/api/v1/web/guest/default/resize"
            local -r URL="$(wsk-act-url "${action}" | awk 'NR==2')"
            if [[ -z "${URL}" ]]; then
                _info "Current deployed actions:"
                wsk-act
                _error "URL for action \"${action}\" not found. Did you used existing action?"
                return 1
            fi
            local -r base_URL="$(dirname "${URL}")"

            # retrieving OpenWhisk (wsk) auth information
            local -r auth="$(wsk-g-auth | awk '{print $3}')"
            if [[ -z "${auth}" ]]; then
                _error "Unable to retrieving auth information using 'wsk property get --auth' command."
                return 1
            fi

            if [[ -z "${3}" ]]; then
                _info "Invoking OpenWhisk action named \"${action}\" using curl."
                _info "Action URL: \"${base_URL}/${2}\"."
                _info "NOTE: for simplicity, response data will be saved without file extension."
                # local -r output="${RAMDISK}/response"
                local -r output="${RAMDISK}/response.txt"
                curl -s -k "${base_URL}/${2}?blocking=true" > "${output}"
                _info "Response saved in \"${output}\"."
                command -v qlmanage &> "${DEV_NULL}" && qlmanage -p "${output}" &> "${DEV_NULL}"
            else
                # checking if file exists
                if [[ ! -e "${3}" || ! -f "${3}" || ! -r "${3}" ]]; then
                    _error "File at \"${3}\" is not a regular file."
                    return 1
                fi
                # checking file extension
                local -r data_basename="$(basename "${3}")"
                local -r data_ext="${data_basename##*.}"
                if [[ "${data_ext}" != 'json' ]]; then
                    _warn "File data received \"${3}\" has not json extension."
                fi
                _info "Invoking OpenWhisk action named \"${action}\" using curl, using json data file at \"${3}\"."
                _info "Action URL: \"${base_URL}/${2}\"."
                _info "NOTE: for simplicity, response data will be saved without file extension."
                # local -r output="${RAMDISK}/response"
                local -r output="${RAMDISK}/response.txt"
                curl -s -k "${base_URL}/${2}?blocking=true" -H "Content-Type: application/json" --data-binary "@${3}" --user "${auth}" \
                    > "${output}"
                _info "Response saved in \"${output}\"."
                command -v qlmanage &> "${DEV_NULL}" && qlmanage -p "${output}" &> "${DEV_NULL}"
            fi
            ;;

        wsk )
            if [[ -z "${3}" ]]; then
                _info "Invoking OpenWhisk action named \"${2}\" using wsk command line utility."
                _info "NOTE: for simplicity, response data will be saved without file extension."
                # local -r output="${RAMDISK}/response"
                local -r output="${RAMDISK}/response.txt"
                wsk-act-binv "${2}" > "${output}"
                _info "Data output in \"${output}\"."
                command -v qlmanage &> "${DEV_NULL}" && qlmanage -p "${output}" &> "${DEV_NULL}"
            else
                # checking if file exists
                if [[ ! -e "${3}" || ! -f "${3}" || ! -r "${3}" ]]; then
                    _error "File at \"${3}\" is not a regular file."
                    return 1
                fi
                # checking file extension
                local -r data_basename="$(basename "${3}")"
                local -r data_ext="${data_basename##*.}"
                if [[ "${data_ext}" != 'json' ]]; then
                    _warn "File data received \"${3}\" has not json extension."
                fi
                _info "Invoking OpenWhisk action named \"${2}\" using wsk command line utility, using json data file at \"${3}\"."
                _info "NOTE: for simplicity, response data will be saved without file extension."
                # local -r output="${RAMDISK}/response"
                local -r output="${RAMDISK}/response.txt"
                wsk-act-binv "${2}" -P "${3}" > "${output}"
                _info "Data output in \"${output}\"."
                command -v qlmanage &> "${DEV_NULL}" && qlmanage -p "${output}" &> "${DEV_NULL}"
            fi
            ;;

        # wsk )
        #     _info "Invoking OpenWhisk action named \"${2}\" using wsk command line utility, binary data file at \"${3}\"."
        #     _info "NOTE: for semplicity, output data (field '__ow_body') will be saved without extension."
        #     wsk-act-binv "${2}" -p '__ow_body' "$(base64 -i "${3}")" | jq -r ".__ow_body" | base64 -D -o "${RAMDISK}/out"
        #     _info "Data output in \"${RAMDISK}/out\"."
        #     ;;

        # wskjson )
        #     local -r data_basename="$(basename "${3}")"
        #     local -r data_ext="${data_basename##*.}"
        #     if [[ "${data_ext}" != 'json' ]]; then
        #         _warn "File data received \"${3}\" has not json extension."
        #     fi
        #     _info "Invoking OpenWhisk action named \"${2}\" using wsk command line utility, json data file at \"${3}\"."
        #     _info "NOTE: for semplicity, output data (field '__ow_body') will be saved without extension."
        #     wsk-act-binv "${2}" -P "${3}" | jq -r '.__ow_body' | base64 -D -o "${RAMDISK}/out"
        #     _info "Data output in \"${RAMDISK}/out\"."
        #     ;;

        * )
            _error "Unknown option \"${1}\" for command query."
            return 1
            ;;
    esac
}

parse_input() {
    if [[ ${#} -eq 0 ]]; then
        usage
        return 1
    fi

    while [[ ${#} -gt 0 ]]; do
        case "${1}" in
            deploy )
                shift
                parse_deploy_input "${1}" "${2}" "${3}" "${4}" || return ${?}
                shift
                shift
                shift
                shift
                ;;

            help | HELP | -h | -H | --help | --HELP )
                usage
                return 0
                ;;

            ow )
                shift
                parse_ow_input "${1}" || return ${?}
                shift
                ;;

            query )
                shift
                parse_query_input "${1}" "${2}" "${3}" || return ${?}
                shift
                shift
                shift
                ;;

            * )
                _error "Unknown command \"${1}\"."
                return 1
                ;;
        esac
    done
}

# launcher
launcher() {

    register_signal

    parse_input "${@}"

}


# call launcher function
launcher "${@}"