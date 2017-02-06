/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceCreationModel = require("models/resources/creation/resourceCreationModel");
import configuration = require("configuration");

class databaseCreationModel extends resourceCreationModel {
    get resourceType() {
        return "database";
    } 

    indexesPath = ko.observable<string>();
    incrementalBackup = ko.observable<boolean>();

    incrementalBackupSettings = {
        alertTimeout: ko.observable<number>(24),
        alertRecurringTimeout: ko.observable<number>(7)
    }

    alertTimeoutOptions = [4, 8, 12, 24, 48, 72];
    alertRecurringTimeoutOptions = [1, 2, 4, 7, 14];

    advancedValidationGroup = ko.validatedObservable({
        dataPath: this.dataPath,
        journalsPath: this.journalsPath,
        tempPath: this.tempPath,
        indexesPath: this.indexesPath
    });

    setupValidation(resourceDoesntExist: (name: string) => boolean) {
        super.setupValidation(resourceDoesntExist);

        this.setupPathValidation(this.indexesPath, "Indexes");
    }

    toDto(): Raven.Abstractions.Data.DatabaseDocument {
        const settings: dictionary<string> = {};
        const securedSettings: dictionary<string> = {};

        if (this.incrementalBackup()) {
            settings[configuration.storage.allowIncrementalBackups] = "true";
        }

        if (this.tempPath() && this.tempPath().trim()) {
            settings[configuration.storage.tempPath] = this.tempPath();
        }

        if (this.dataPath() && this.dataPath().trim) {
            settings[configuration.core.dataDirectory] = this.dataPath();
        }

        if (this.indexesPath() && this.indexesPath().trim()) {
            settings[configuration.indexing.storagePath] = this.indexesPath();
        }

        if (this.journalsPath() && this.journalsPath().trim()) {
            settings[configuration.storage.journalsStoragePath] = this.journalsPath();
        }

        /* TODO
                if (!clusterWide) settings["Raven-Non-Cluster-Database"] = "true";

        //TODO: return alertTimeout as null if left with default values (24/7)
                if (alertTimeout !== "") {
                    settings["Raven/IncrementalBackup/AlertTimeoutHours"] = alertTimeout;
                }
                if (alertRecurringTimeout !== "") {
                    settings["Raven/IncrementalBackup/RecurringAlertTimeoutDays"] = alertRecurringTimeout;
                }
        */

        this.fillEncryptionSettingsIfNeeded(securedSettings);

        return {
            Id: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false
        };
    }

    setAlertTimeout(value: number) {
        this.incrementalBackupSettings.alertTimeout(value);
    }

    setRecurringAlertTimeout(value: number) {
        this.incrementalBackupSettings.alertRecurringTimeout(value);
    }
    
}

export = databaseCreationModel;
