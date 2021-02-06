#
alias launcher="$(realpath "$(dirname "${BASH_SOURCE}")")/launcher.sh"  # @doc: Shortcut for launcher.sh script
#
alias launcher-inst='launcher ow install && launcher ow wa'  # @doc: Install openwhisk using launcher command
#
alias launcher-uninst='launcher ow uninstall'  # @doc: Uninstall openwhisk instation using launcher command
#
alias redis_ip="$(realpath "$(dirname "${BASH_SOURCE}")")/redis_ip.sh"  # @doc: Shortcut for redis_ip.sh script
#
alias kafka_ip="$(realpath "$(dirname "${BASH_SOURCE}")")/kafka_ip.sh"  # @doc: Shortcut for kafka_ip.sh script
#
alias docker-build="$(realpath "$(dirname "${BASH_SOURCE}")")/docker_build.sh"  # @doc: Shortcut for docker_build.sh script
#
alias sched-coll="$(realpath "$(dirname "${BASH_SOURCE}")")/stdout_collector.sh -s scheduler"  # @doc: Create a file with output of docker attach to specified container

#
alias docker-build-controller="$(realpath "$(dirname "${BASH_SOURCE}")/../res/openwhisk-priority-queue/script/docker_build.sh")"  # @doc: Creare docker image for Apache OpenWhisk project