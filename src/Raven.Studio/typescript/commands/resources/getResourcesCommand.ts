import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import resourcesInfo = require("models/resources/info/resourcesInfo");

class getResourcesCommand extends commandBase {
    
    execute(): JQueryPromise<resourcesInfo> {
        const url = endpoints.global.resources.resources;

        const resultSelector = (info: Raven.Client.Data.ResourcesInfo) => new resourcesInfo(info);
        return this.query(url, null, null, resultSelector);
    }
}

export = getResourcesCommand;
