import resource = require("models/resources/resource");

interface resourceDisconnectedEventArgs {
    resource: resource;
    cause: resourceDisconnectionCause;
}

export = resourceDisconnectedEventArgs;