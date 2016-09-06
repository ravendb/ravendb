import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class saveClusterConfigurationCommand extends commandBase {

    constructor(private dto: clusterConfigurationDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Cluster configuration.");
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved Cluster configuration."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save cluster configuration.", response.responseText, response.statusText));
    }

    private saveSetup(): JQueryPromise<any> {
        var putArgs = JSON.stringify(this.dto);
        return this.put("/admin/cluster/commands/configuration", putArgs, this.db, { dataType: undefined });//TODO: use endpoints
    }
}

export = saveClusterConfigurationCommand;
