# ============================================================================
# Titolo: helm.sh
# Descrizione: Contiene alias utili per progetti
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:04:59 CET 2020
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



#######################################
##### Helm
##### see@ https://helm.sh/docs/

#
alias helm='/usr/local/opt/helm/helm'  # @doc: -/-



# Automatic parse and display docs for aliases
# Args: none
# Deps: none
helm_help() {
    _als_quick_help "${BASH_SOURCE}"
}