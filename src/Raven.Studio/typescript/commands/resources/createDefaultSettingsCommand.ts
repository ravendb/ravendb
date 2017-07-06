import database = require("models/resources/database");
import document = require("models/database/documents/document");
import commandBase = require("commands/commandBase");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/resources/saveDatabaseSettingsCommand");

class createDefaultSettingsCommand extends commandBase {
    constructor(private db: database, private bundles: string[]) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Creating default settings for '" + this.db.name + "'...");

        var tasksToWatch: Array<JQueryPromise<any>> = []; 
        if (_.includes(this.bundles, "Quotas")) {
            tasksToWatch.push(this.updateQuotasSettings());
        }
        /* TODO
        if (_.includes(this.bundles, "Versioning")) {
            tasksToWatch.push(this.saveVersioningConfiguration());
        }*/

        if (tasksToWatch.length > 0) {
            return $.when.apply(null, tasksToWatch);
        } else {
            return $.Deferred().resolve();
        }

    }

    private fillDefaultQuotasSettings(doc: document): document {
        var result: any = new document(doc.toDto(true));
        result["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = (50 * 1024).toString(); 
        result["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = (45 * 1024).toString();
        result["Settings"]["Raven/Quotas/Documents/HardLimit"] = (10000).toString();
        result["Settings"]["Raven/Quotas/Documents/SoftLimit"] = (8000).toString();
        return result;
    }

    private saveDatabaseSettings(databaseSettings: document) {
        return new saveDatabaseSettingsCommand(this.db, databaseSettings).execute();
    }

    private updateQuotasSettings(): JQueryPromise<any> {
        var taskDone = $.Deferred();

        new getDatabaseSettingsCommand(this.db, false)
            .execute()
            .fail(() => taskDone.fail())
            .then(this.fillDefaultQuotasSettings)
            .then((doc) => this.saveDatabaseSettings(doc))
            .fail(() => taskDone.fail())
            .then(() => taskDone.resolve());
      
        return taskDone;
    }

    /* TODO
    private createDefaultVersioningSettings(): Array<versioningEntry> {
        return [
            new versioningEntry({
                Id: "DefaultConfiguration",
                MaxRevisions: 5,
                Exclude: false,
                ExcludeUnlessExplicit: false,
                PurgeOnDelete: false
            })
        ];
    }*/

    /*TODO
    private saveVersioningConfiguration(): JQueryPromise<any> {

        var saveTask = $.Deferred();
        this.hasGlobalVersioningSettings().fail(() => saveTask.fail())
            .done((has: boolean) => {
                if (has) {
                    // use global settings - nothing to do 
                    saveTask.resolve();
                } else {
                    var entries: Array<versioningEntryDto> = this.createDefaultVersioningSettings()
                        .map((ve: versioningEntry) => ve.toDto(true));
                    new saveVersioningCommand(this.db, entries).execute()
                        .done(() => saveTask.resolve())
                        .fail(() => saveTask.reject());
                }
            });
        return saveTask;
    }*/


}

export = createDefaultSettingsCommand;
