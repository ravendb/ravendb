import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class saveReplicationDocumentCommand extends commandBase {

    constructor(private dto: replicationsDto, private db: database, private globalConfig = false) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Replication settings.");
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved Replication settings."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Replication settings.", response.responseText, response.statusText));
    }

    private saveSetup(): JQueryPromise<any> {
        var id = this.globalConfig ? "Raven/Global/Replication/Destinations" : "Raven/Replication/Destinations";
        var url = "/document?id=" + id;
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, this.db);
    }
}

export = saveReplicationDocumentCommand;
