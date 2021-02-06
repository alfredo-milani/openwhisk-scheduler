# ============================================================================
# Titolo: faas_profiler.sh
# Descrizione: Contiene alias utili per progetti
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:04:59 CET 2020
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================


#######################################
##### FaasProfiler
##### see@ http://parallel.princeton.edu/FaaSProfiler.html

#
export FAAS_PROF='${PROJ}/FaaS/faas-profiler'
#
alias faasprof-inv='python3 "${FAAS_PROF}"/synthetic-workload-invoker/WorkloadInvoker.py -c "${FAAS_PROF}"/workload.json'  # @doc: Call synthetic invoker with configuration file
alias faasprof-an='python3 "${FAAS_PROF}"/synthetic-workload-invoker/WorkloadAnalyzer.py'  # @doc: Call workload analyzer
alias faasprof-can='python3 "${FAAS_PROF}"/synthetic-workload-invoker/ComparativeAnalyzer.py'  # @doc: Call comparative analyzer


# Automatic parse and display docs for aliases
# Args: none
# Deps: none
faas_prof_help() {
    _als_quick_help "${BASH_SOURCE}"
}