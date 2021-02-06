see@ https://docs.docker.com/docker-for-mac/install/
Relationship to Docker Machine: Installing Docker Desktop on Mac does not affect machines you created with Docker Machine.
You have the option to copy containers and images from your local default machine (if one exists) to the Docker Desktop HyperKit VM. 
When you are running Docker Desktop, you do not need Docker Machine nodes running locally (or anywhere else). 
With Docker Desktop, you have a new, native virtualization system running (HyperKit) which takes the place of the VirtualBox system.

see@ https://github.com/moby/hyperkit
HyperKit is a toolkit for embedding hypervisor capabilities in your application.
It includes a complete hypervisor, based on xhyve/bhyve, which is optimized for lightweight virtual machines and container deployment. 
It is designed to be interfaced with higher-level components such as the VPNKit and DataKit.
HyperKit currently only supports macOS using the Hypervisor.framework. It is a core component of Docker Desktop for Mac.