# ============================================================================
# Titolo: kubernetes.sh
# Descrizione: Contiene alias utili per progetti
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:04:59 CET 2020
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



#######################################
##### Kubernetes
##### see@ https://kubernetes.io/it/docs/
##### see@ https://kubernetes.io/docs/reference/kubectl/cheatsheet/

# 
alias kc='/usr/local/opt/kubernetes-cli/kubectl'  # @doc: -/-
#
alias kc-ci='kc cluster-info'  # @doc: Get cluster info
#
alias kc-g-ns='kc get namespaces'  # @doc: List namespaces
# 
alias kc-g-ps='kc get pod -A'  # @doc: Get cluster information: view pods status
#
alias kc-g-pi='kc get pod -o wide -n'  # @doc: Get more information aboud pods, e.g. kc-pi namesapceName
#
alias kc-g-svc='kc get svc'  # @doc: Show services
#
alias kc-g-s='kc get services'  # @doc: Show services of ImageID
#
alias kc-g-ip='kc get service --output='jsonpath="{.spec.ports[0].nodePort}"''  # @doc: Get Kubernetes VM IP exposed to the host system. Any serviced of type NodePort can be accessed over that IP address, on the NodePort
#
alias kc-d-s='kc describe svc'  # @doc: Show all information about ServiceID
#
alias kc-d-d='kc describe deployment'  # @doc: Show description of DeploymentID
#
alias kc-c-d='kc create deployment'  # @doc: Create deplyment of ServiceID with ImageID
#
alias kc-e-d='kc expose deployment'  # @doc: Expose deployment of ServiceID using TypeID and PortID


# Automatic parse and display docs for aliases
# Args: none
# Deps: none
kc_help() {
    _als_quick_help "${BASH_SOURCE}"
}