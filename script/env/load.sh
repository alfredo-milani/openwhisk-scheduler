# ============================================================================
# Titolo: load.sh
# Descrizione: Init script
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:03:44 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



# Load all modules from ${MOD}
# Args:
# -c [opt]: check files' dependencies before sourcing.
# -e [opt]: call "shopt -s expand_aliases" to deal with aliases in non-interactive shells.
# -s [opt]: exit if command return with non-zero value
# -t [opt]: skip shell type check
# -v [opt]: skip shell version ckeck
# @dep: basename dirname printf
mod_init() {  # @doc: Load scripts for bash environment.

	# parsing input
	local check_dep=false
	local expand_aliases=false
	local stop_error=false
	local shell_version_check=true
	local shell_type_check=true
	while [[ ${#} -gt 0 ]]; do
		case "${1}" in
			-c | --check-dep )
				check_dep=true
				shift
				;;

			-e | --expand-aliases )
				expand_aliases=true
				shift
				;;

			-h | -H | --help | --HELP )
                cat <<EOF
${BASH_SOURCE} [-opt]

Options

    -c | --check-dep
        Check files' dependencies before sourcing.
    -e | --expand-aliases
        Expand aliases, to deal with non-interactive shells.
    -h | -H | --help | --HELP
        Print current help.
    -s | --stop-error
        Set option to exit if a command return with non-zero value.
    -t | --skip-shell-type-check
    	Skip shell type check.
    -v | --skip-shell-version-check
    	Skip shell version check.
EOF
				return 0
                ;;

			-s | --stop-error )
				stop_error=true
				shift
				;;

			-t | --skip-shell-type-check )
				shell_type_check=false
				shift
				;;

			-v | --skip-shell-version-check )
				shell_version_check=false
				shift
				;;

			* )
				printf "[ERROR] - Unknown option \"${1}\"."
				return 1
				;;
		esac
	done

	# check supported shell (currently only bash shell)
	if [[ "${shell_type_check}" == true ]]; then
		# see@ https://unix.stackexchange.com/questions/170493/login-non-login-and-interactive-non-interactive-shells
		# see@ https://stackoverflow.com/questions/3327013/how-to-determine-the-current-shell-im-working-on
		local -r shell='bash'
		local -r shell_current="$(basename ${SHELL})"
		if [[ "${shell}" != "${shell_current}" ]]; then
			printf "[ERROR] - Invalid shell. Current: \"${shell_current}\", supported: \"${shell}\".\n"
			return 1
		fi
	fi

	# check shell version
	if [[ "${shell_version_check}" == true ]]; then
		# minimum bash version supported
		local bash_version_minimum="4.0.0(1)-release"
		local bash_version_minimum="${bash_version_minimum%(*}"
		# put version in array
		local -r bash_version_minimum_array=(${bash_version_minimum//./ })
		# from version number, e.g. 4.4.19(1)-release, remove text from '(' onwards
		local -r bash_version="${BASH_VERSION%(*}"
		# put version number in array
		local -r bash_version_array=(${bash_version//./ })
		# check bash version
		for i in ${!bash_version_minimum_array[@]}; do
			[[ ${bash_version_array[${i}]} -gt ${bash_version_minimum_array[${i}]} ]] && break
			if [[ ${bash_version_array[${i}]} -lt ${bash_version_minimum_array[${i}]} ]]; then
				printf "[ERROR] - Minimun bash version required \"${bash_version_minimum}\", current \"${bash_version}\"\n"
				return 1
			fi
		done
	fi

	# filename of this script
	local -r script_basename="$(basename ${BASH_SOURCE})"
	# directory containing all modules
	local -r mod="$(dirname ${BASH_SOURCE})"
	# directory containing core modules
	local -r core="${mod}/.core"
	# declare core components
	local -r environment="${core}/environment.sh"
	local -r alias="${core}/alias.sh"
	local -r function="${core}/function.sh"

	# load core components or return error
	if ! source "${environment}"; then
		printf "[ERROR] - Can not source \"${environment}\": file does not exists or contains syntactic error.\n"
		return 1
	elif ! source "${alias}"; then
		printf "[ERROR] - Can not source \"${alias}\": file does not exists or contains syntactic error.\n"
		return 1
	elif ! source "${function}"; then
		printf "[ERROR] - Can not source \"${function}\": file does not exists or contains syntactic error.\n"
		return 1
	fi

	# check if ${mod} and ${MOD} corresponds to the same filename or return error
	if [[ ! "${mod}" -ef "${MOD}" ]]; then
		_error "Root directory of script \"${script_basename}\" doesn't match with that declared in \"${environment}\"."
		return 1
	elif [[ ! -d "${MOD}" ]]; then
		_error "\"${MOD}\" is not a valid directory."
		return 1
	fi

	# modules to source
	local modules=''
    for dir in "${MOD}/"*; do
		if [[ -d "${dir}" && "${dir}" != "${core}" ]]; then
			# remove ${MOD} base path
			local module="${dir#"${MOD}"}"
			# remove first char '/'
			module="${module:1:${#module}}"
			# add to modules
			modules+="${module}:"
		fi
    done

    local args=()
    # expand aliases
    [[ "${expand_aliases}" == true ]] && shopt -s expand_aliases
    # exit if command return with non-zero value
    [[ "${stop_error}" == true ]] && set -e
    # check files' dependencies before sourcing
    [[ "${check_dep}" == true ]] && args+=('-c' "$(basename "${core}")" '-c' "${modules}")

    args+=('-s' "${modules}")
    # recurively checking dependencies and source all modules, excluding core module already sourced
    modl "${args[@]}"

    # cleanup
    unset -f "${FUNCNAME}"

}


### This script should be sourced from ~/.bash_profile, ~/.bashrc or ~/.profile, depending on specific use case.
mod_init "${@}" || return ${?}