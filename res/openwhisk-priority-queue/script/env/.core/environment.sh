# ============================================================================
# Titolo: environment.sh
# Descrizione: Contiene la definizione delle variabili di ambiente per una shell bash
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:01:50 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================


# Core directory
export MOD='/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/res/openwhisk-priority-queue/script/env'
# Supported extension, e.g. MOD_EXT=('sh' 'ksh')
# Note: extensions can not contains space, e.g. 'g t' is an invalid extension type
export MOD_EXT=('sh')
# Extension for disabled modules
export MOD_OFF='.off'

# Change prompt style
# PS1_OLD="${PS1}"
export PS1='\[\e[1m\]\[\e[32m\]\u\[\e[0m\]\[\e[1m\]@\[\e[32m\]\h\[\e[0m\]\[\e[1m\]:\[\e[1m\]\[\e[32m\]\w \[\e[0;32m\]$\[\e[0m\] '
# Per stampare nel titolo delle finestre dei terminali la directory corrente
export PROMPT_COMMAND='echo -ne "\033]0;${PWD}\007"'

# Null device
export DEV_NULL='/dev/null'
