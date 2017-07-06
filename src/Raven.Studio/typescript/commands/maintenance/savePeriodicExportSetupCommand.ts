import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import periodicExportSetup = require("models/database/documents/periodicExportSetup");

class savePeriodicExportSetupCommand extends commandBase {

    constructor(private setupToPersist: periodicExportSetup, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Periodic Export setup.");
        return $.when(this.saveAccountInformation(), this.saveSetup())
            .done(() => this.reportSuccess("Saved Periodic Export setup."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Periodic Export setup.", response.responseText));
    }

    private saveAccountInformation(): JQueryPromise<any> {
        var jQueryOptions: JQueryAjaxSettings = {};
        if (this.setupToPersist.getEtag()) {
            jQueryOptions.headers = {
                'If-None-Match': this.setupToPersist.getEtag()
            }
        }
        var url = "/admin/databases?name=" + this.db.name;//TODO: use endpoints
        var putArgs = JSON.stringify(this.setupToPersist.toDatabaseSettingsDto());
        return this.put(url, putArgs, null, jQueryOptions);
    }

    private saveSetup(): JQueryPromise<any> {
        return $.Deferred();
        /*var url = "/docs?id=Raven/Backup/Periodic/Setup";//TODO: use endpoints
        var putArgs = JSON.stringify(this.setupToPersist.toDto());
        return this.put(url, putArgs, this.db);*/
    }
}
export = savePeriodicExportSetupCommand;
