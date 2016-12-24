import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getResourceCommand extends commandBase {

    constructor(private resourceType: string, private resourceName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Data.ResourceInfo> {

        const url = endpoints.global.resources.resource;
        return this.query<Raven.Client.Data.ResourceInfo>(url, { type: this.resourceType, name: this.resourceName }, null);
    }
}

export = getResourceCommand;
