
# This script registers a route to Docker subnet through DockerNAT virtual ethernet interface
# Needs to be run with elevated privileges

$DOCKER_NETWORK = "172.17.0.0"
$BRIDGE_NETWORK = "10.0.75.2"
route add "$DOCKER_NETWORK" mask 255.255.0.0 "$BRIDGE_NETWORK" -p
