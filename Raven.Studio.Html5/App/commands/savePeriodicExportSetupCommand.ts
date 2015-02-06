import commandBase = require("commands/commandBase");
import database = require("models/database");
import periodicExportSetup = require("models/periodicExportSetup");

class savePeriodicExportSetupCommand extends commandBase {

    constructor(private setupToPersist: periodicExportSetup, private db: database, private globalConfig = false) {
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
        var url = this.globalConfig? "/configuration/global/settings" : "/admin/databases/" + this.db.name;
        var putArgs = JSON.stringify(this.setupToPersist.toDatabaseSettingsDto());
        return this.put(url, putArgs, null, jQueryOptions);
    }

    private saveSetup(): JQueryPromise<any> {
        var url = this.globalConfig ? "/docs/Raven/Global/Backup/Periodic/Setup" : "/docs/Raven/Backup/Periodic/Setup";
        var putArgs = JSON.stringify(this.setupToPersist.toDto());
        return this.put(url, putArgs, this.db);
    }
}
export = savePeriodicExportSetupCommand;