# ============================================================================
# Titolo: function.sh
# Descrizione: Contiene definizioni di funzioni
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 05:59:17 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================


#######################################################
### Core components
#######################################################

# Function for logging
# Args
# ${*} [opt]: message to print
# @dep:
_log() {
	printf "\e[34m[LOG]\033[0m ${FUNCNAME[1]:-unknown} - ${*}\n"
}
# Function for display generic informations
# Args
# ${*} [opt]: message to print
# @dep:
_info() {
	printf "\e[1m[INFO]\033[0m - ${*}\n"
}
# Function for warning
# Args
# ${*} [opt]: message to print
# @dep:
_warn() {
	printf >&2 "\033[1;33m[WARN]\033[0m - ${*}\n"
}
# Function for error
# Args
# ${*} [opt]: message to print
# @dep:
_error() {
	printf >&2 "\033[0;31m[ERROR]\033[0m ${FUNCNAME[1]:-unknown} - ${*}\n"
}
# Function for abort
# Args
# ${*} [opt]: message to print
# @dep:
_abort() {
	printf >&2 "\033[0;31m[ABORT]\033[0m ${FUNCNAME[1]:-unknown} - ${*}\n"
	exit 1
}

# Function that looks for dependencies in source code.
# All dependencies found will be added to array ${MOD_DEP}.
# The doc string *must* follows this schema:
#  e.g.
#  # @dep: sed grep sort
# Note 1: comments can not be multiline.
# Note 2: a space after '# @dep:' is required
# Args
# {1} [mnd]: source code to parse looking for dependercies
# @dep: grep sed tr sort
_dep_finder() {
	[[ -z "${1}" || ! -f "${1}" ]] && _error "File \"${1}\" does not exists." && return 1

	# LANG=en_US.UTF_8
	# merge new found dependencies with older array declared by caller
	MOD_DEP+=( $(grep -E '^# @dep' "${1}" | sed -e 's|# @dep:||g' | tr ' ' '\n' | sort -u) )
}

# Tool to check if input string are valid commands.
# All missing dependencies will be added to array ${MOD_DEP_MISS}
# Args
# ${@}: commands to find in the system, generally "${MOD_DEP[@]}"
# @dep:
_dep_missing() {
	for cmd in "${@}"; do
		! command -v "${cmd}" &> "${DEV_NULL}" && MOD_DEP_MISS+=("${cmd}")
	done
}

# Function for automatically get brief description about function module.
# The doc string *must* follows this schema:
#  e.g.
#  test_function() {  # @doc: comments starts here and requires two spaces from bracket and one space after colon
#    : function body here
#  }
# Note 1: comments can not be multiline.
# Note 2: _fn_quick_help and _als_quick_help can not be called from the same script
#  beacuse they use different parsing tecniques.
# Args
# ${1} [mnd]: file (generally ${BASH_SOURCE}) to read in order to construct docs
# @dep: grep sed column sort
_fn_quick_help() {  # @doc: Show brief description for bash functions right marked with '@doc:' string
	[[ -z "${1}" || ! -f "${1}" ]] && _error "File \"${1}\" does not exists." && return 1

	# LANG=en_US.UTF_8
	{ printf "FUNCTION ƒ DESCRIPTION\n" &&
	# excludes comments and lines starting with 'grep'
	# see@ https://unix.stackexchange.com/questions/194848/how-to-run-multiple-and-in-grep-command
	# see@ https://superuser.com/questions/537619/grep-for-term-and-exclude-another-term
	grep '^[[:blank:]]*[^[:blank:]#]' < "${1}" | grep -Fv 'grep' | grep '() {  # @doc' | \
	sed -e 's| {  # @doc:|ƒ|g'; } | \
	column -s 'ƒ' -t | \
	sort
}

# Function for automatically get brief description about alias module.
# The doc string *must* follows this schema:
#  e.g.
#  alias reload='source ~/.bash_profile'  # @doc: comments starts here and requires two spaces from the end of alias and one space after colon
# Note 1: comments can not be multiline.
# Note 2: _als_quick_help and _fn_quick_help can not be called from the same script
#  beacuse they use different parsing tecniques.
# Args
# ${1} [mnd]: file (generally ${BASH_SOURCE}) to read in order to construct docs
# @dep: grep sed column
_als_quick_help() {  # @doc: Show brief description for bash aliases right marked with '@doc:' string
	[[ -z "${1}" || ! -f "${1}" ]] && _error "File \"${1}\" does not exists." && return 1

	# LANG=en_US.UTF_8
	{ printf "ALIAS ƒ COMMAND ƒ DESCRIPTION\n" &&
	# excludes comments, get only alias which have @doc: string 
	grep '^[[:blank:]]*[^[:blank:]#]' < "${1}" | grep '^alias\s' | grep '  # @doc:' | \
	sed -e 's|  # @doc:|ƒ|g' -e 's|alias ||g' -e 's|=|ƒ|'; } | \
	column -s 'ƒ' -t # | \
	# sort
}

# Function which check wheather ${1} directory contains file.
# Hidden files will not be counted.
# This function uses subprocess for checking emptiness.
# Args
# ${1} [mnd]: directory filename
# Return
# ${?}: 0 if directory at ${1} is empty, an integer greater than 0 otherwise
# @dep:
_is_dir_empty() (  # @doc: Return 0 if specified directory is empty, an integer greater than 0 otherwise
	shopt -s nullglob
	set -- "${1}/"*
	return ${#}
)

# Function which iterates from second positional argument onwards:
#  - if it is a directory: iterate recursively over all files (or directories);
#  - if it is a file: evaluate command received as first positional argument against current file.
# The variable ${file} can be used to perform action on found file.
# Hidden files or directories will not be considered.
# Args
# ${1} [mnd]: command to evaluate using var "${file}" to access current file
# ${@:2} [mnd]: files or directory to inspect
# @dep: ls grep awk column
recevalf() {  # @doc: Iterate over input (files and directories recusively) and evaluate command ${1} over found files (files can be accessed with ${file} variable)
	if [[ -z "${1}" ]]; then
		_error "${FUNCNAME[0]}: empty action to perform."
		return 1
	fi

	for file in "${@:2}"; do
		# directory to inspect
		if [[ -d "${file}" ]]; then
			! _is_dir_empty "${file}" && recevalf "${1}" "${file}/"*
		# command evaluation
		elif [[ -f "${file}" ]]; then
			eval "${1}"
		# not a regular file or directory
		else
			_warn "File \"${file}\" is not a regular file or directory."
		fi
	done
}

# Function which iterates from second positional argument onwards:
#  - if it is a directory: evaluate command received as firt positional argument against current directory;
#      iterate recursively over all directories, if current directory has not be deleted and theare are still reading permission;
# The variable ${dir} can be used to perform action on found directory.
# Hidden files or directories will not be considered.
# Args
# ${1} [mnd]: command to evaluate using var "${dir}" to access current directory
# ${@:2} [mnd]: files or directory to inspect
# @dep: ls grep awk column
recevald() {  # @doc: Iterate over input (files and directories recusively) and evaluate command ${1} over found directory (directory can be accessed with ${dir} variable)
	if [[ -z "${1}" ]]; then
		_error "${FUNCNAME[0]}: empty action to perform."
		return 1
	fi

	for dir in "${@:2}"; do
		# command evaluation
		if [[ -d "${dir}" ]]; then
			eval "${1}"
			# directory deleted
			[[ ! -e "${dir}" ]] && continue
			# lost reading permission
			[[ ! -r "${dir}" ]] && continue
			! _is_dir_empty "${dir}" && recevald "${1}" "${dir}/"*
		# not a regular file nor directory
		elif [[ ! -f "${dir}" ]]; then
			_warn "File \"${dir}\" is not a regular file or directory."
		fi
	done
}

# Manages shell modules
# @dep: ls grep awk column
#
# TODO [BUG]
# - errore ls: con filenames separati da spazi da errore
# - errore column: se i filename hanno spazi, vengono considerate come colonne aggiuntive
# - utilizzare la var d'ambiente MOD_EXT per il filtering dei moduli attivi
# TODO [IMPROVEMENT]
# - inserire file load.sh in ogni collezione di moduli (directory) per esprimere ordine di sourcing
modl() {  # @doc: Manages shell modules, e.g. modl -f util/util.sh  # source util.sh file

	local usage
	read -r -d '' usage << EOF
${FUNCNAME[0]} [-opt]

	-c dir/module.sh | --check-dep dir/module.sh
		Check if all dependencies specified in doc string are satisfied.
		Unsatisfied dependencies will be shown.
		Will be even checked disabled modules.
	-d dir/module.sh | --disable dir/module.sh
		Disable specified modules for all future shells.
		Note that you must not specify base path "${MOD}".
		e.g. ${FUNCNAME[0]} -d util/util.sh:media/media.sh

		If it is specified a directory, all modules within directory will be disabled.
		e.g. ${FUNCNAME[0]} -d util  # disable all modules within 'util' directory
	-e dir/module.sh | --enable dir/module.sh
		Enable specified modules (supported extensions: '${MOD_EXT}') for all future shells and for the current.
		e.g. ${FUNCNAME[0]} -e util/util.sh:media/media.sh  # enable util/util.sh and media/media.sh modules

		If it is specified a directory, all modules within directory will be enabled and sourced.
		e.g. ${FUNCNAME[0]} -e util  # enable all modules within 'util' directory
		e.g. ${FUNCNAME[0]} -e util -e helper  # enable all modules within 'util' and 'helper' directory
	-f dir/module.sh | --force-source dir/module.sh
		Load selected modules only for current shell.
		These files will not be eneabled for future shells.
		Files will be sourced even if they does not match with "${MOD_EXT}" or 
		 if they are marked with "${MOD_DIS}".
	-h
		Print current help
	-l | --list
		List all modules.
	-le | --list-enabled
		List all enabled modules.
	-ld | --list-disabled
		List all disabled modules.
	-s dir/module.sh | --source dir/module.sh
		Load selected modules. Modules marked with "${MOD_DIS}" or whose not matching 
		 with "${MOD_EXT}" will not be considered.
		Note that you must not specify base path "${MOD}".
		e.g. ${FUNCNAME[0]} -s util/util.sh

		If it is specified a directory, all modules within directory will be sourced.
		e.g. ${FUNCNAME[0]} -s util  # source all modules within 'util' directory
EOF
	
	local modules_to_check=()
	local modules_to_enable=()
	local modules_to_disable=()
	local modules_to_source=()
	local modules_to_force_source=()
	local listing=''

	# set IFS with special char to parse multiple file at once
	local -r IFS_OLD="${IFS}"
	IFS=':'
	while [[ ${#} -gt 0 ]]; do
		case "${1}" in
			-c | --check-dep )
				shift
				[[ -n "${1}" ]] && modules_to_check+=(${1})
				shift
				;;

			-d | --disable )
				shift
				[[ -n "${1}" ]] && modules_to_disable+=(${1})
				shift
				;;

			-e | --enable )
				shift
				[[ -n "${1}" ]] && modules_to_enable+=(${1})
				shift
				;;

			-f | --force-source )
				shift
				[[ -n "${1}" ]] && modules_to_force_source+=(${1})
				shift
				;;

			-h | -H | --help | --HELP )
				printf "${usage}\n"
				return 0
				;;

			-l | --list )
				listing='all'
				shift
				;;

			-le | --list-enabled )
				listing='enabled'
				shift
				;;

			-ld | --list-disabled )
				listing='disabled'
				shift
				;;
			
			-s | --source )
				shift
				[[ -n "${1}" ]] && modules_to_source+=(${1})
				shift
				;;

			* )
				_error "Flag \"${1}\" not valid."
				return 1
				;;
		esac
	done
	# set IFS to previous value
	IFS="${IFS_OLD}"

	if [[ ${#modules_to_check[@]} -gt 0 ]]; then
		set -- \
			'local MOD_DEP=()
			local MOD_DEP_MISS=()
			# find dependencies
			_dep_finder "${file}"
			# check missing dependencies
			_dep_missing "${MOD_DEP[@]}"
			# show missing dependencies to the user if present
			[[ ${#MOD_DEP_MISS[@]} -gt 0 ]] && _warn "File \"${file}\" requires: ${MOD_DEP_MISS[*]}"' \
			"${modules_to_check[@]/#/${MOD}/}"  # prefix each module with its absolute path
		# check if input files contains unsatisfied dependencies
		recevalf "${@}"
	fi

	if [[ ${#modules_to_disable[@]} -gt 0 ]]; then
		set -- \
			'local file_basename="$(basename "${file}")"
			local ext="${file_basename#*.}"
			# if file extension is not supported then skip current file
			[[ ! " ${MOD_EXT[@]} " =~ " ${ext} " ]] && continue
			mv "${file}" "${file}${MOD_OFF}"' \
			"${modules_to_disable[@]/#/${MOD}/}"  # prefix each module with its absolute path
		# disable only supported files specified in ${MOD_EXT} environment variable
		recevalf "${@}"
	fi

	if [[ ${#modules_to_enable[@]} -gt 0 ]]; then
		set -- \
			'if [[ "${file:(-${#MOD_OFF})}" == "${MOD_OFF}" ]]; then
				local old_file="${file}"
				# creating filename without ${MOD_OFF} extension
				file="${file:0:$((${#file} - ${#MOD_OFF}))}"
				# extracting extension
				local file_basename="$(basename "${file}")"
				local ext="${file_basename#*.}"
				# if file extension is not supported then skip current file
				[[ ! " ${MOD_EXT[@]} " =~ " ${ext} " ]] && continue
				mv "${old_file}" "${file}"
				# source file
				source "${file}"
			fi' \
			"${modules_to_enable[@]/#/${MOD}/}"  # prefix each module with its absolute path
		# enable all files ending with "{MOD_OFF}" environment variable
		recevalf "${@}"
	fi

	if [[ ${#modules_to_source[@]} -gt 0 ]]; then
		set -- \
			'local file_basename="$(basename "${file}")"
			local ext="${file_basename#*.}"
			# if file extension is not supported then skip current file
			[[ ! " ${MOD_EXT[@]} " =~ " ${ext} " ]] && continue
			# source file
			source "${file}"' \
			"${modules_to_source[@]/#/${MOD}/}"  # prefix each module with its absolute path
		# source only supported files specified in ${MOD_EXT} environment variable
		recevalf "${@}"
	fi

	if [[ ${#modules_to_force_source[@]} -gt 0 ]]; then
		set -- \
			'# source file
			source "${file}"' \
			"${modules_to_force_source[@]/#/${MOD}/}"  # prefix each module with its absolute path
		# source only supported files specified in ${MOD_EXT} environment variable
		recevalf "${@}"
	fi

	# list files contained in ${MOD}, if user requested it
	if [[ -n "${listing}" ]]; then
		case "${listing}" in
			'all' )
				local base_mod_len=$(( ${#MOD} + 2 ))
				ls -l $(find "${MOD}" -type f \( ! -iname ".*" ! -iname "*.txt" \)) | 
				grep -v "load.sh\|.core" | 
				awk -v base_mod_len="${base_mod_len}" '{module = substr($9, base_mod_len); print $1, $3, $4, module}' |
				column -t -s ' ' -c 4 -x
				;;

			'enabled' )
				# find "${MOD}" -type f -ls \( -iname "*.sh" \) -and \( ! -iname ".*" ! -iname "*load*" ! -iname "*core*" \) | 
				local base_mod_len=$(( ${#MOD} + 2 ))
				ls -l $(find "${MOD}" -type f \( ! -iname ".*" ! -iname "*.txt" \)) | 
				grep -v "load.sh\|.core" | 
				awk -v base_mod_len="${base_mod_len}" '{module = substr($9, base_mod_len); print $1, $3, $4, module}' |
				grep -E '\.sh$' |
				column -t -s ' ' -c 4 -x
				;;

			'disabled' )
				local base_mod_len=$(( ${#MOD} + 2 ))
				ls -l $(find "${MOD}" -type f \( ! -iname ".*" ! -iname "*.txt" \)) | 
				grep -v "load.sh\|.core" | 
				awk -v base_mod_len="${base_mod_len}" '{module = substr($9, base_mod_len); print $1, $3, $4, module}' |
				grep -E '\.off$' |
				column -t -s ' ' -c 4 -x
				;;

			* )
				_error "Mode \"${listing}\" not supported."
				return 1
				;;
		esac
	fi

}

# Automatic parse and display docs for aliases
# Args: none
# @dep:
core_fn_help() {
	_fn_quick_help "${BASH_SOURCE}"
}