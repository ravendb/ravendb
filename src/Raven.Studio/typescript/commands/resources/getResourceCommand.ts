import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import resourceInfo = require("models/resources/info/resourceInfo");

class getResourceCommand extends commandBase {

    constructor(private rs: resourceInfo) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Data.ResourceInfo> {

        const url = endpoints.global.resources.resource;
        return this.query<Raven.Client.Data.ResourceInfo>(url, { name: this.rs.name, type: this.rs.qualifier}, null);
    }
}

export = getResourceCommand;
