# ============================================================================
# Titolo: alias.sh
# Descrizione: Contiene definizioni di alias
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:03:44 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================


# refresh shell
alias reload='source ~/.bash_profile'  # @doc: Reload ~/.bash_profile

# note that the space after 'watch' command is necessary to enable alias expansion, if any
alias wa='watch -e -n 1 '  # @doc: Watch output changes of the specified command, e.g. wa 'ls -all'
alias l='ls'
alias la='ls -h -all'
# ls alias for color-mode
alias lh='ls -lhaG'
# grep with color
alias grep='grep --color=auto'
# ps show processes
# alias ps='ps -ax'
alias psf='ps -A | grep'
# up 'n' folders
alias ..='cd ..;'
alias ...='.. ..'
alias ....='... ..'
alias .....='... ...'


# Automatic parse and display docs for aliases
# Args: none
# @dep:
core_als_help() {
    _als_quick_help "${BASH_SOURCE}"
}
