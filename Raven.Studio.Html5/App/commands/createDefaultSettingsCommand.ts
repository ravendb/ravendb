import database = require("models/database");
import document = require("models/document");
import versioningEntry = require("models/versioningEntry");

import commandBase = require("commands/commandBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import getVersioningsCommand = require("commands/getVersioningsCommand");
import saveVersioningCommand = require("commands/saveVersioningCommand");

class createDefaultSettingsCommand extends commandBase {
    constructor(private db: database, private bundles) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Creating default settings for '" + this.db.name + "'...");
        
        var tasksToWatch = []; 
        if (this.bundles.contains("Quotas")) {
            tasksToWatch.push(this.updateQuotasSettings());
        }
        if (this.bundles.contains("Versioning")) {
            tasksToWatch.push(this.saveVersioningConfiguration());
        }

        if (tasksToWatch.length > 0) {
            return $.when.apply(null, tasksToWatch);
        } else {
            return $.Deferred().resolve();
        }

    }

    private fillDefaultQuotasSettings(doc: document): document {
        var result = new document(doc.toDto(true));
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
    }

    private saveVersioningConfiguration(): JQueryPromise<any> {
        var entries: Array<versioningEntryDto> = this.createDefaultVersioningSettings()
            .map((ve: versioningEntry) => ve.toDto(true));

        return new saveVersioningCommand(this.db, entries).execute();
    }
}

export = createDefaultSettingsCommand;