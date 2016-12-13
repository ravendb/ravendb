import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/resources/saveDatabaseSettingsCommand");

class enableReplicationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var task = $.Deferred();
        // query for database document
        new getDatabaseSettingsCommand(this.db, false)
            .execute()
            .done((dbSettings) => {
                var activeBundles = dbSettings.Settings["Raven/ActiveBundles"];
                if (activeBundles && activeBundles.indexOf("Replication") > -1) {
                    // looks like we already have replication enabled - nothing to do
                    task.resolve(activeBundles.split(";"));
                    this.reportSuccess("Replication was already enabled for this database");
                } else {
                    if (!activeBundles) {
                        activeBundles = "Replication";
                    } else {
                        activeBundles += ";Replication";
                    }

                    dbSettings.__metadata.etag = dbSettings.__metadata["@etag"];

                    // and save updated settings back to server
                    dbSettings.Settings["Raven/ActiveBundles"] = activeBundles;
                    dbSettings.__metadata["Raven-Temp-Allow-Bundles-Change"] = true;
                    new saveDatabaseSettingsCommand(this.db, dbSettings)
                        .execute()
                        .done(() => {
                            task.resolve(activeBundles.split(";"));
                        }).fail(() => {
                            task.reject();
                        });
                    

                }
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to enable replication bundle!", response.responseText, response.statusText);
                task.reject();
            });

        return task.promise();
    }
}

export = enableReplicationCommand;
