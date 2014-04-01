import commandBase = require("commands/commandBase");
import database = require("models/database");
import periodicBackupSetup = require("models/periodicBackupSetup");
import appUrl = require("common/appUrl");

class savePeriodicBackupSetupCommand extends commandBase {

    constructor(private setupToPersist: periodicBackupSetup, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Periodic Backup setup.");
        return jQuery.when(this.saveAccountInformation(), this.saveSetup())
            .done(() => this.reportSuccess("Saved Periodic Backup setup."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Periodic Backup setup.", response.responseText));
    }

    private saveAccountInformation(): JQueryPromise<any> {
        var url = "/admin/databases/" + this.db.name;
        var putArgs = JSON.stringify(this.setupToPersist.toDatabaseSettingsDto());
        return this.post(url, putArgs, null, { dataType: undefined });
    }

    private saveSetup(): JQueryPromise<any> {
        var url = "/docs/Raven/Backup/Periodic/Setup";
        var putArgs = JSON.stringify(this.setupToPersist.toDto());
        return this.put(url, putArgs, this.db);
    }
}
export = savePeriodicBackupSetupCommand;