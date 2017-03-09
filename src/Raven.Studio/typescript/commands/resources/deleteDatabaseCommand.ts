import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteResourceCommand extends commandBase {

    constructor(private databases: Array<database>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Server.Web.System.ResourceDeleteResult>> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        const args = {
            "hard-delete": this.isHardDelete,
            name: this.databases.map(x => x.name)
        };

        return this.del<Raven.Server.Web.System.ResourceDeleteResult[]>(url + this.urlEncodeArgs(args), null, null, 9000 * this.databases.length)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete resources", response.responseText, response.statusText));
    }


} 

export = deleteResourceCommand;
