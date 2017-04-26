/// <reference path="../../../../typings/tsd.d.ts"/>

import configuration = require("configuration");

class databaseCreationModel {

    name = ko.observable<string>("");

    indexesPath = ko.observable<string>();
    incrementalBackup = ko.observable<boolean>();

    incrementalBackupSettings = {
        alertTimeout: ko.observable<number>(24),
        alertRecurringTimeout: ko.observable<number>(7)
    }

    alertTimeoutOptions = [4, 8, 12, 24, 48, 72];
    alertRecurringTimeoutOptions = [1, 2, 4, 7, 14];

    dataPath = ko.observable<string>();
    journalsPath = ko.observable<string>();
    tempPath = ko.observable<string>();

    activeBundles = ko.observableArray<string>([]);

    advancedValidationGroup = ko.validatedObservable({
        dataPath: this.dataPath,
        journalsPath: this.journalsPath,
        tempPath: this.tempPath,
        indexesPath: this.indexesPath
    });

    globalValidationGroup = ko.validatedObservable({
        name: this.name
    });

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        observable.extend({
            maxLength: {
                params: maxLength,
                message: `Path name for '${name}' can't exceed ${maxLength} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `{0} path can't contain any of the following characters: * ? " < > |`,
                params: name
            },
            {
                validator: (val: string) => !rg3.test(val),
                message: `The name {0} is forbidden for use!`,
                params: this.name
            }]
        });
    }

    setupValidation(databaseDoesntExist: (name: string) => boolean) {
        const rg1 = /^[^\\/:\*\?"<>\|]*$/; // forbidden characters \ / : * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        this.setupPathValidation(this.dataPath, "Data");
        this.setupPathValidation(this.tempPath, "Temp");
        this.setupPathValidation(this.journalsPath, "Journals");

        this.name.extend({
            required: true,
            maxLength: 230,
            validation: [
                {
                    validator: databaseDoesntExist,
                    message: "Database already exists"
                }, {
                    validator: (val: string) => rg1.test(val),
                    message: `The database name can't contain any of the following characters: \\ / : * ? " < > |`,
                }, {
                    validator: (val: string) => !val.startsWith("."),
                    message: `The database name can't start with a dot!`
                }, {
                    validator: (val: string) => !val.endsWith("."),
                    message: `The database name can't end with a dot!`
                }, {
                    validator: (val: string) => !rg3.test(val),
                    message: `The name {0} is forbidden for use!`,
                    params: this.name
                }]
        });

        this.setupPathValidation(this.indexesPath, "Indexes");
    }

    toDto(): Raven.Client.Documents.DatabaseRecord {
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

        return {
            DatabaseName: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false
        } as Raven.Client.Documents.DatabaseRecord;
    }

    setAlertTimeout(value: number) {
        this.incrementalBackupSettings.alertTimeout(value);
    }

    setRecurringAlertTimeout(value: number) {
        this.incrementalBackupSettings.alertRecurringTimeout(value);
    }
    
}

export = databaseCreationModel;
