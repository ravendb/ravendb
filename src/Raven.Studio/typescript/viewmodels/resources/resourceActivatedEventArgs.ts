import resource = require("models/resources/resource");

interface resourceActivatedEventArgs {
    resource: resource;
}

export = resourceActivatedEventArgs;