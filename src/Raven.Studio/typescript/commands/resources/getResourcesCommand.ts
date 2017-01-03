import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getResourcesCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.Data.ResourcesInfo> {
        const url = endpoints.global.resources.resources;

        return this.query(url, null);
    }
}

export = getResourcesCommand;
