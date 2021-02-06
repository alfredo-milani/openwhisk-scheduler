# ============================================================================
# Titolo: docker.sh
# Descrizione: Contiene alias utili per progetti
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:04:59 CET 2018
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================



#######################################
##### Docker
##### see@ https://docs.docker.com/engine/reference/commandline/docker/

#
alias dk='docker'  # @doc: -/-
#
alias dk-sys-prune='docker system prune'  # @doc: -/-
#
alias dk-stop-all-c='docker stop $(docker ps -a -q)'  # @doc: Stop all containers
#
alias dk-img='docker images'  # @doc: Shows all Docker images on the system
#
alias dk-r='docker run'  # @doc: Download Docker image, if it is not already present, and run as a container
#
alias dk-r-int='docker run -it'  # @doc: As dk-r, but in interactive mode
#
alias dk-r-det='docker run -d'  # @doc: Run Docker in detached mode
#
alias dk-rmi='docker rmi'  # @doc: Remove image on the system with specified ImageID
#
alias dk-insp='docker inspects'  # @doc: Get information about Repository (image or container)
#
alias dk-ps='docker ps'  # @doc: Get current running containers
#
alias dk-ps-a='docker ps -a'  # @doc: Get all containers
#
alias dk-his='docker history'  # @doc: Get command history about specified ImageID

#
alias dk-top='docker top'  # @doc: Get top processes of ContainerID
#
alias dk-stop='docker stop'  # @doc: Stop specified ContainerID
#
alias dk-rm-c='docker rm'  # @doc: Remove specificed ContainerID
#
alias dk-stat='docker stats'  # @doc: Get stats of ContainerID
#
alias dk-att='docker attach'  # @doc: Attach terminal’s stdin, stdout, stderr to a running container with ContainerID, e.g. dk-att ContainerID
#
alias dk-kill='docker kill'  # @doc: Kill container with ContainerID

#
alias dk-net='docker network'  # @doc: -/-
#
alias dk-net-c='docker network create'  # @doc: e.g. dk-net-c new_network
#
alias dk-net-list='docker network list'  # @doc: -/-
#
alias dk-net-of='docker inspect -f "{{json .NetworkSettings.Networks }}"'  # @doc: -/-
#
alias dk-net-disc='docker network disconnect'  # @doc: Disconnect Network from ContainerID
#
alias dk-net-conn='docker network connect'  # @doc: Connect Container to Network
#
alias dk-net-rm='docker network rm'  # @doc: Remove NetworkID
#
alias dk-net-rm-all='docker network prune'  # @doc: Remove all unused networks
#
alias dk-net-cont-conn='docker network inspect -f "{{json .Containers }}"'  # @doc: Get containers on NetworkID

#
alias dk-vol='docker volume'  # @doc: -/-
#
alias dk-vol-c='docker volume create'  # @doc: -/-
#
alias dk-vol-list='docker volume list'  # @doc: -/-
#
alias dk-vol-rm='docker volume rm'  # @doc: -/-
#
alias dk-vol-rm-all='docker volume prune'  # @doc: Remove all unused volumes

#
alias dk-cp='docker cp'  # @doc: Copy files/directory from/to container, e.g. dk-cp src_file containerID:dst_dir ; or dk-cp containerID:src_file dst_dir
#
alias dk-x='docker exec -it'  # @doc: Execute Command on ContainerID, e.g. dk-x ContainerID Command
#
alias dk-cmd-all='docker inspect -f "{{.Name}} {{.Config.Cmd}}" $(docker ps -a -q --no-trunc)'  # @doc: -/-

#
alias dkc='docker-compose'  # @doc: -/-
#
alias dkc-ps='docker-compose ps'  # @doc: -/-
#
alias dkc-u='docker-compose up -d'  # @doc: -/-


dk-tags() {  # @doc: Show tags for all running containers
    docker inspect $(docker ps  | awk '{print $2}' | grep -v ID) | jq .[].RepoTags  
}



# Automatic parse and display docs for aliases
# Args: none
# Deps: none
dk_help() {
    _als_quick_help "${BASH_SOURCE}"
    echo
    _fn_quick_help "${BASH_SOURCE}"
}