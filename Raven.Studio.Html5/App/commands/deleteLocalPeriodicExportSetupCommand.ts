import commandBase = require("commands/commandBase");
import database = require("models/database");
import periodicExportSetup = require("models/periodicExportSetup");

class deleteLocalPeriodicExportSetupCommand extends commandBase {

    constructor(private setupToPersist: periodicExportSetup, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Updating Periodic Export setup.");
        return $.when(this.saveAccountInformation(), this.deleteSetup())
            .done(() => this.reportSuccess("Updated Periodic Export setup."))
            .fail((response: JQueryXHR) => this.reportError("Failed to update Periodic Export setup.", response.responseText));
    }

    private saveAccountInformation(): JQueryPromise<any> {
        var jQueryOptions: JQueryAjaxSettings = {};
        if (this.setupToPersist.getEtag()) {
            jQueryOptions.headers = {
                'If-None-Match': this.setupToPersist.getEtag()
            }
        }

        var url = "/admin/databases/" + this.db.name;
        
        var putArgs = JSON.stringify(this.setupToPersist.removeDatabaseSettings());
        return this.put(url, putArgs, null, jQueryOptions);
    }

    private deleteSetup(): JQueryPromise<any> {
        var url = "/docs/Raven/Backup/Periodic/Setup";
        return this.del(url, null, this.db);
    }
}
export = deleteLocalPeriodicExportSetupCommand;