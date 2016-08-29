import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class initializeNewClusterCommand extends commandBase {


    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.patch("/admin/cluster/initialize-new-cluster", null, this.db)//TODO: use endpoints
            .done(() => this.reportSuccess("New cluster has been initialized"))
            .fail((response: JQueryXHR) => this.reportError("Unable to initialize new cluster", response.responseText, response.statusText));
    }
}

export = initializeNewClusterCommand;
