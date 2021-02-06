#!/usr/bin/env bash
# ============================================================================
# Titolo: fork.sh
# Descrizione: Fork base environment
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:03:44 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



CORE_DIR='.core'
ENV_FILE='environment.sh'
CORE_MODULES=("${CORE_DIR}" 'load.sh' 'README.txt' 'fork.sh')


usage() {
    local usage
    read -r -d '' usage << EOF
${BASH_SOURCE} [-opt]
    
    Create a fork of current bash environment.

OPTIONS

    -a | --add-modules
        Add modules, specified using -m flag, to already forked bash environment.
    -d '/dest' | --destination '/dest'
        Creare base environment in '/dest' directory.
    -h | -H | --help | --HELP
        Print current help.
    -m 'module' | --modules 'module'
        Specify modules to include during forking.
        Can be specified multile modules at once using this syntax: e.g. ${FUNCNAME[0]} -m 'dev/java:dev/kubernetes'.
    -o | --replace-mod-only
        Modify file './${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path' specified using -r flag.
    -r '/path' | --replace-mod '/path'
        Set custom path for base path of bash modules (\${MOD}) environment variable.

EXAMPLES
    
    ${BASH_SOURCE}
        Fork current base bash environment in current directory.
    ${BASH_SOURCE} -d '/dest'
        Fork current base bash environment in '/dest' directory.
    ${BASH_SOURCE} -r '/path'
        Fork current base bash environment in current directory and modify file './${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path'.
    ${BASH_SOURCE} -m 'dev/docker:dev/kubernetes'
        Fork current base bash environment in current directory and copy specified modules.
    ${BASH_SOURCE} -d '/dest' -m 'dev/docker'
        Fork current base bash environment in '/dest' and copy specified modules.
    ${BASH_SOURCE} -d '/dest' -r '/path'
        Fork current base bash environment in '/dest' and modify file '/dest/${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path'.
    ${BASH_SOURCE} -m 'dev/docker' -r '/path'
        Fork current base bash environment in current directory, copy specified modules and modify file '/dest/${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path'.
    ${BASH_SOURCE} -d '/dest' -m 'dev/docker' -r '/path'
        Fork current base bash environment in '/dest', copy specified modules and modify file '/dest/${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path'.
    ${BASH_SOURCE} -a -m 'dev/docker'
        Add specified modules in current directory.
    ${BASH_SOURCE} -o -r '/path'
        Modify file './${CORE_DIR}/${ENV_FILE}', replacing the value for \${MOD} field with '/path'.
EOF
    
    printf "${usage}\n"
}

parse_input() {
    local IFS=':'
    while [[ ${#} -gt 0 ]]; do
        case "${1}" in
            -a | --add-modules )
                ADD_MODULES=true
                shift
                ;;

            -d | --destination )
                shift
                DESTINATION="$(realpath "${1}")"
                shift
                ;;

            -h | -H | --help | --HELP )
                usage
                exit 0
                ;;

            -m | --modules )
                shift
                [[ -n "${1}" ]] && MODULES+=(${1})
                shift
                ;;

            -o | --replace-mod-only )
                REPLACE_MOD_ONLY=true
                shift
                ;;

            -r | --replace-mod )
                shift
                MOD_FORK="${1}"
                shift
                ;;

            * )
                printf "[ERROR] - Unknown option \"${1}\".\n"
                return 1
                ;;
        esac
    done
}

infer_actions() {
    # default behaviour
    FORK_CORE=true
    FORK_MODULES=true
    REPLACE_MOD=true

    # only add specified modules
    if [[ "${ADD_MODULES}" == true ]]; then
        FORK_CORE=false
        REPLACE_MOD=false
    fi

    # only modify environment.sh file
    if [[ "${REPLACE_MOD_ONLY}" == true ]]; then
        FORK_CORE=false
        FORK_MODULES=false
    fi
}

validate_input() {
    # if no specified otherwise, using current directory as destination
    if [[ -z "${DESTINATION}" ]]; then
        DESTINATION="${PWD}"
    # create missing destination, if necessary
    elif [[ ! -e "${DESTINATION}" ]]; then
        printf "Creating missing destination directory in \"${DESTINATION}\".\n"
        mkdir -p "${DESTINATION}"
        if [[ ${?} != 0 ]]; then
            printf "[ERROR] - Can not create directory at \"${DESTINATION}\".\n"
            return 1
        fi
    # write permission on destination required
    elif [[ ! -r "${DESTINATION}" ]]; then
        printf "[ERROR] - Missing write permission on \"${DESTINATION}\".\n"
        return 1
    # destination can not be a file
    elif [[ -f "${DESTINATION}" ]]; then
        printf "[ERROR] - Destination can not be a file. Current: \"${DESTINATION}\".\n"
        return 1
    fi

    # using default destination if no path is specified for new base environment
    if [[ -z "${MOD_FORK}" ]]; then
        MOD_FORK="${DESTINATION}"
    fi

    if [[ "${FORK_CORE}" == true && -e "${DESTINATION}/${CORE_DIR}" ]]; then
        read -p "In current directory already exists '.core' directory. Continue anyway? [ yes | no ] > " choose
        if [[ "${choose}" != 'Yes' && "${choose}" != 'yes' ]]; then
            exit 0
        fi
    fi
}

# Using rsync to mantain directory structure when copying files directly.
# Note that onn MacOS Mojave "$ sed -i '' "|export MOD=|s|.*$|${MOD_FORK}|" "${DESTINATION}/${CORE_DIR}/${ENV_FILE}""
#   does not works (neither using : insted of |).
process() {
    if [[ "${FORK_CORE}" == true ]]; then
        printf "* Forking base bash environment, from \"${MOD}\" to \"${DESTINATION}\".\n"
        # using rsync to mantain directory structure when copying files directly
        rsync --recursive -ptgoD --relative "${CORE_MODULES[@]/#/${MOD}/./}" "${DESTINATION}" 1> '/dev/null' || return 1
    fi

    if [[ "${FORK_MODULES}" == true ]]; then
        printf "* Copying specified modules, from \"${MOD}\" to \"${DESTINATION}\".\n"
        # using rsync to mantain directory structure when copying files directly
        rsync --recursive -ptgoD --relative "${MODULES[@]/#/${MOD}/./}" "${DESTINATION}" 1> '/dev/null' || return 1
    fi

    if [[ "${REPLACE_MOD}" == true ]]; then
        printf "* Setting bash environment path to \"${MOD_FORK}\".\n"
        # replacing base ${MOD}
        sed -i '' "s|^export MOD=.*$|export MOD='${MOD_FORK}'|" "${DESTINATION}/${CORE_DIR}/${ENV_FILE}" || return 1
    fi   
}

integrity_check() {
    if [[ -z "${MOD}" || ! -d "${MOD}" || ! -r "${MOD}" ]]; then
        printf "[ERROR] - Invalid \${MOD} environment variable. Can not fork current base environment.\n"
        return 1
    fi
}

# Fork base environment.
# @dep: printf rsync sed
fork() {  # @doc: Fork base environment

    integrity_check

    # actions supported
    local FORK_CORE=false
    local FORK_MODULES=false
    local REPLACE_MOD=false
    local ADD_MODULES=false
    local REPLACE_MOD_ONLY=false

    # variables containing user's input
    local MODULES=()
    local DESTINATION=''
    local MOD_FORK=''

    parse_input "${@}" || return 1
    infer_actions || return 1
    validate_input || return 1

    process || return 1

}


# create new base environment
fork "${@}"