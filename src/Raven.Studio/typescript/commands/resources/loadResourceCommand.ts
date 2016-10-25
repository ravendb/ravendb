import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class loadResourceCommand extends commandBase {

    constructor(private rs: resource) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = this.getQueryUrlFragment();
        return this.query<void>(url, null, this.rs, null, this.getTimeToAlert(true));
    }

    private getQueryUrlFragment(): string {
        if (this.rs instanceof database) {
            return endpoints.databases.stats.stats;
        }
        throw new Error("I don't know how to load: " + this.rs.qualifier);
    }
}

export = loadResourceCommand;
