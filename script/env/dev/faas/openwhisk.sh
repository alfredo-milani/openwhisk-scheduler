# ============================================================================
# Titolo: openwhisk.sh
# Descrizione: Contiene alias utili per progetti
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:04:59 CET 2020
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



#######################################
##### OpenWhisk
##### see@ https://openwhisk.apache.org/documentation.html

#
alias wsk='/usr/local/opt/OpenWhisk_CLI-1.1.0-mac-amd64/wsk'  # @doc: -/-
#
alias wski='wsk -i'  # @doc: Use OpenWhisk in insecure mode
#
alias wsk-g-auth='wsk property get --auth'  # @doc: Show configured auth propery
#
alias wsk-g-apihost='wsk property get --apihost'  # @doc: Show configured apihost propery
#
alias wsk-act='wski action list'  # @doc: Show all available actions (functions)
#
alias wsk-act-sort='wski action list --name-sort'  # @doc: List all actions (functions) in alphabetical order
#
alias wsk-act-inv='wski action invoke'  # @doc: Invoke OpenWhisk action, e.g. wsk-act-inv /whisk.sysytem/samples/greeting
#
alias wsk-act-create='wski action create'  # @doc: Create action, e.g. wsk-act-create greeting greeting.js
#
alias wsk-act-ud='wski action update'  # @doc: Update action, e.g. wsk-act-up greeting greeting.js
#
alias wsk-act-get='wski action get'  # @doc: Get metadata that describes existing actions, e.g. wsk-act-get greeting
#
alias wsk-act-del='wski action delete'  # @doc: Delete specified action, e.g. wsk-act-del greeting
#
alias wsk-actv-rec='wski activation get'  # @doc: Get activation record, e.g. wsk-act-rec 31b580428454401fb580428454601f88
#
alias wsk-actv-res='wski activation result'  # @doc: Get activation result, e.g. wsk-act-res 31b580428454401fb580428454601f88
#
alias wsk-actv-log='wski activation logs'  # @doc: Get activation logs, e.g. wsk-act-log 31b580428454401fb580428454601f88
#
alias wsk-actv-poll='wski activation poll'  # @doc: Watch the output of actions as they are invoked
#
alias wsk-actv-list='wski activation list'  # @doc: List a series of activation records
#
alias wsk-nspace='wski namespace list -v'  # @doc: Get namespace value for current user


#
wsk-act-save() {  # @doc: Save deployed action code to file, e.g. wsk-act-save greeting /dir/greeting.js; wsk-act-save greeting
    if [[ -z "${1}" ]]; then
        _error "Action name can not be empty"
        return 1
    fi

    if [[ -z "${2}" ]]; then
        wsk-act-get "${1}" --save
    else
        wsk-act-get "${1}" --save-as "${2}"
    fi
}

# @dep: basename
wsk-act-createe() {  # @doc: Create all action specified, e.g. wsk-act-createe login.js /path1/failure.js /path2/success.js
    for file in "${@}"; do
        local file_basename="$(basename "${file}")"
        local action="${file_basename%.*}"
        wsk-act-create "${action}" "${file}"
    done
}
#
wsk-act-ud-all() {  # @doc: Update all OpenWhisk currently deployed actions, with specified concurrency level, e.g. wsk-act-ud-all 5  # update all actions using 5 as concurrency level
    local -r concurrency="${1}"
    if [[ -z "${concurrency}" ]]; then
        _error "Concurrency level must be specified."
        return 1
    elif [[ "${concurrency}" -lt 0 ]]; then
        _error "Concurrency limit can not be lower then 0."
        return 1
    fi

    # retrieve currenty deployed actions
    local -r actions="$(wsk-act | awk '{print $1}' | awk 'NR>1')"
    
    if [[ -z "${actions}" ]]; then
        _info "No action to update."
        return 0
    fi

    while IFS= read -r action; do
        _info "Updating \"${action}\" action with concurrency limit ${concurrency}."
        wsk-act-ud "${action}" --concurrency "${concurrency}"
    done <<< "${actions}"
}
#
wsk-act-dell() {  # @doc: Delete all action specified, e.g. wsk-act-dell login failure success
    for action in "${@}"; do
        wsk-act-del "${action}"
    done
}
#
wsk-act-del-all() {  # @doc: Delete all OpenWhisk currently deployed actions
    # retrieve currenty deployed actions
    local -r actions="$(wsk-act | awk '{print $1}' | awk 'NR>1')"

    # show currenty deployed actions
    _info "Currently deployed actions:"
    printf "${actions}\n\n"
    
    if [[ -z "${actions}" ]]; then
        _info "No action to delete."
        return 0
    fi

    read -p "[DANGER] - Delete all currently deployed actions? [yes | no] > " choose
    if [[ "${choose}" != 'yes' && "${choose}" != 'YES' ]]; then
        return 0
    fi

    while IFS= read -r action; do
        _info "Deleting \"${action}\" action."
        wsk-act-del "${action}"
    done <<< "${actions}"
}
#
wsk-act-url() {  # @doc: Retrieve action URL, e.g. wsk-act-url greeting
    local -r action="${1?Action name required, e.g. ${FUNCNAME} greeting}"
    wsk-act-get "${@}" --url
}
#
wsk-act-binv() {  # @doc: Invoke specified action in blocking mode and show result (if there are no errors), e.g. wsk-act-binv greeting
    local -r action="${1?Action name required, e.g. ${FUNCNAME} greeting}"
    wsk-act-inv "${@}" --result
}
# @dep: npx basename
wsk-cmp-cmp() {  # @doc: Call openwhisk-composer (JavaScript) compose command to create composition. Be sure to be in the installation directory of composer. E.g. wsk-cmp-cmp ../../composition1.js /path/dir/composition2.js
    for file in "${@}"; do
        local -r file_basename="$(basename "${file}")"
        local -r file_ext="${file_basename##*.}"
        local -r composition="${file_basename%.*}"

        if [[ "${file_ext}" != 'js' ]]; then
            _warn "File \"${file}\" is not in JS format. Skipping..."
            continue
        fi

        npx compose "${file}" --file || return 1
    done
}
# @dep: npx basename
wsk-cmp-dp() {  # @doc: Call openwhisk-composer (JavaScript) deploy command to create composition. Be sure to be in the installation directory of composer. E.g. wsk-cmp-cmp ../../composition1.json /path/dir/composition2.json 
    for file in "${@}"; do
        local -r file_basename="$(basename "${file}")"
        local -r file_ext="${file_basename##*.}"
        local -r composition="${file_basename%.*}"

        if [[ "${file_ext}" != 'json' ]]; then
            _warn "File \"${file}\" is not in JSON format. Skipping..."
            continue
        fi

        npx deploy "${composition}" "${file}" -i || return 1
    done
}


# Automatic parse and display docs for aliases
# Args: none
# Deps: none
wsk_help() {
    _als_quick_help "${BASH_SOURCE}"
    printf '\n'
    _fn_quick_help "${BASH_SOURCE}"
}