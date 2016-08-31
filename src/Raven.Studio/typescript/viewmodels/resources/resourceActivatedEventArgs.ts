import resource = require("models/resources/resource");

interface resourceActivatedEventArgs {
    type: TenantType;
    resource: resource;
}

export = resourceActivatedEventArgs;