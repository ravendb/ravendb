import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");

class monitorRestoreCommand extends commandBase {
    constructor(private parentPromise: JQueryDeferred<any>, private updateRestoreStatus: (restoreStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        new getDocumentWithMetadataCommand("Raven/Restore/Status", null)
            .execute()
            .fail((response: JQueryXHR) => {
                setTimeout(() => this.execute(), 1000);
            })
            .done((restoreStatus: restoreStatusDto)=> {
                this.updateRestoreStatus(restoreStatus);

                if (restoreStatus.State == "Running") {
                    setTimeout(() => this.execute(), 1000);
                } else {
                    if (restoreStatus.State == "Completed") {
                        this.reportSuccess("Database was successfully restored!");
                        this.parentPromise.resolve();
                    } else {
                        this.reportError("Database wasn't restored!");
                        this.parentPromise.reject();
                    }
                }
            });
        return this.parentPromise;
    }
}

export = monitorRestoreCommand;
