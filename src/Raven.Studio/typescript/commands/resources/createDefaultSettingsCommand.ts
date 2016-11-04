import database = require("models/resources/database");
import document = require("models/database/documents/document");
import versioningEntry = require("models/database/documents/versioningEntry");

import commandBase = require("commands/commandBase");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/resources/saveDatabaseSettingsCommand");
import saveVersioningCommand = require("commands/database/documents/saveVersioningCommand");
import getEffectiveVersioningsCommand = require("commands/database/globalConfig/getEffectiveVersioningsCommand");
import configurationDocument = require("models/database/globalConfig/configurationDocument");
import getConfigurationSettingsCommand = require("commands/database/globalConfig/getConfigurationSettingsCommand");
import configurationSettings = require("models/database/globalConfig/configurationSettings");

class createDefaultSettingsCommand extends commandBase {
    constructor(private db: database, private bundles: string[]) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Creating default settings for '" + this.db.name + "'...");

        var tasksToWatch: Array<JQueryPromise<any>> = []; 
        if (this.bundles.contains("Quotas")) {
            tasksToWatch.push(this.updateQuotasSettings());
        }
        /* TODO
        if (this.bundles.contains("Versioning")) {
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
        this.hasGlobalQuotaSettings().fail(() => taskDone.fail())
            .done((has: boolean) => {
                if (has) {
                    // use global settings - nothing to do
                    taskDone.resolve();
                } else {
                    new getDatabaseSettingsCommand(this.db, false)
                        .execute()
                        .fail(() => taskDone.fail())
                        .then(this.fillDefaultQuotasSettings)
                        .then((doc) => this.saveDatabaseSettings(doc))
                        .fail(() => taskDone.fail())
                        .then(() => taskDone.resolve());
                }
            });

      
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

    /* TODO

    private hasGlobalVersioningSettings(): JQueryPromise<boolean> {
        var hasGlobal = $.Deferred();
        new getEffectiveVersioningsCommand(this.db)
            .execute()
            .done((data: configurationDocument<versioningEntry>[]) => {
                hasGlobal.resolve(!!data.first(config => config.globalExists()));
            })
            .fail(() => hasGlobal.reject());

        return hasGlobal.promise();
    }*/

    private hasGlobalQuotaSettings(): JQueryPromise<boolean> {
        var hasGlobal = $.Deferred();
        new getConfigurationSettingsCommand(this.db,
            ["Raven/Quotas/Size/HardLimitInKB"])
            .execute()
            .done((result: configurationSettings) => {
                // note: we detect presence of global config based on single property!
                var hardLimit = result.results["Raven/Quotas/Size/HardLimitInKB"];
                hasGlobal.resolve(hardLimit.globalExists());
            })
            .fail(() => hasGlobal.reject());

        return hasGlobal.promise();
    }
}

export = createDefaultSettingsCommand;
