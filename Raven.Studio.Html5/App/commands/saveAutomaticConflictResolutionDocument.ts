import commandBase = require("commands/commandBase");
import document = require("models/document");
import database = require("models/database");

class saveAutomaticConflictResolutionDocument extends commandBase {

    constructor(private dto: replicationConfigDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving automatic conflict resolution settings.");
        return this.saveConfig()
            .done(() => this.reportSuccess("Saved automatic conflict resolution settings."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save automatic conflict resolution settings.", response.responseText));
    }

    private saveConfig(): JQueryPromise<any> {
        var id = "Raven/Replication/Config";
        var url = "/docs/" + id;
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, this.db);
    }
}

export = saveAutomaticConflictResolutionDocument; 