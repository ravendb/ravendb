import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import changesApi = require('common/changesApi');
import shell = require('viewmodels/shell');

class databases extends viewModelBase {

    databases = ko.observableArray<database>();
    searchText = ko.observable("");
    selectedDatabase = ko.observable<database>();
    isAnyDatabaseSelected: KnockoutComputed<boolean>;
    allCheckedDatabasesDisabled: KnockoutComputed<boolean>;
    systemDb: database;
    docsForSystemUrl: string;
    optionsClicked = ko.observable<boolean>(false);

    constructor() {
        super();
        
        this.databases = shell.databases;
        this.systemDb = appUrl.getSystemDatabase();
        this.docsForSystemUrl = appUrl.forDocuments(null, this.systemDb);
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterDatabases(s));

        var currentDatabse = this.activeDatabase();
        if (!!currentDatabse) {
            this.selectDatabase(currentDatabse, false);
        }

        this.isAnyDatabaseSelected = ko.computed(() => {
            for (var i = 0; i < this.databases().length; i++) {
                var db: database = this.databases()[i];
                if (db.isChecked()) {
                    return true;
                }
            }

            return false;
        });

        this.allCheckedDatabasesDisabled = ko.computed(() => {
            var disabledStatus = null;
            for (var i = 0; i < this.databases().length; i++) {
                var db: database = this.databases()[i];
                if (db.isChecked()) {
                    if (disabledStatus == null) {
                        disabledStatus = db.disabled();
                    } else if (disabledStatus != db.disabled()) {
                        return null;
                    }
                }
            }

            return disabledStatus;
        });
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    attached() {
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
        this.databasesLoaded();
    }

    private databasesLoaded() {
        // If we have no databases (except system db), show the "create a new database" screen.
        if (this.databases().length === 1) {
            this.newDatabase();
        } else {
            // If we have just a few databases, grab the db stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            var enabledDatabases: database[] = this.databases().filter((db: database) => !db.disabled());
            if (enabledDatabases.length < few) {
                enabledDatabases.forEach(db => shell.fetchDbStats(db));
            }
        }
    }

    getDocumentsUrl(db: database) {
        return appUrl.forDocuments(null, db);
    }

    selectDatabase(db: database, activateDatabase: boolean = true) {
        if (this.optionsClicked() == false) {
            this.databases().forEach((d: database) => d.isSelected(d.name === db.name));
            if (activateDatabase) {
                db.activate();
            }
            this.selectedDatabase(db);
        }

        this.optionsClicked(false);
    }

    newDatabase() {
        // Why do an inline require here? Performance.
        // Since the database page is the common landing page, we want it to load quickly.
        // Since the createDatabase page isn't required up front, we pull it in on demand.
        require(["viewmodels/createDatabase"], createDatabase => {
            var createDatabaseViewModel = new createDatabase(this.databases, shell.licenseStatus);
            createDatabaseViewModel
                .creationTask
                .done((databaseName: string, bundles: string[], databasePath: string, databaseLogs: string, databaseIndexes: string) => {
                    var settings = {
                        "Raven/ActiveBundles": bundles.join(";")
                    };
                    settings["Raven/DataDir"] = (!this.isEmptyStringOrWhitespace(databasePath)) ? databasePath : "~/Databases/" + databaseName;
                    if (!this.isEmptyStringOrWhitespace(databaseLogs)) {
                        settings["Raven/Esent/LogsPath"] = databaseLogs;
                    }
                    if (!this.isEmptyStringOrWhitespace(databaseIndexes)) {
                        settings["Raven/IndexStoragePath"] = databaseIndexes;
                    }

                    this.showDbCreationAdvancedStepsIfNecessary(databaseName, bundles, settings);
                });
            app.showDialog(createDatabaseViewModel);
        });
    }

    showDbCreationAdvancedStepsIfNecessary(databaseName: string, bundles: string[], settings: {}) {
        var securedSettings = {};
        var savedKey;

        var encryptionDeferred = $.Deferred();

        if (bundles.contains("Encryption")) {
            require(["viewmodels/createEncryption"], createEncryption => {
                var createEncryptionViewModel = new createEncryption();
                createEncryptionViewModel
                    .creationEncryption
                    .done((key: string, encryptionAlgorithm: string, encryptionBits: string, isEncryptedIndexes: string) => {
                        savedKey = key;
                        securedSettings = {
                            'Raven/Encryption/Key': key,
                            'Raven/Encryption/Algorithm': this.getEncryptionAlgorithmFullName(encryptionAlgorithm),
                            'Raven/Encryption/KeyBitsPreference': encryptionBits,
                            'Raven/Encryption/EncryptIndexes': isEncryptedIndexes
                        };
                        encryptionDeferred.resolve(securedSettings);
                    });
                app.showDialog(createEncryptionViewModel);
            });
        } else {
            encryptionDeferred.resolve();
        }

        encryptionDeferred.done(() => {
            require(["commands/createDatabaseCommand"], createDatabaseCommand => {
                new createDatabaseCommand(databaseName, settings, securedSettings)
                    .execute()
                    .done(() => {
                        var newDatabase = this.addNewDatabase(databaseName);
                        this.selectDatabase(newDatabase);

                        var encryptionConfirmationDialogPromise = $.Deferred();
                        if (!jQuery.isEmptyObject(securedSettings)) {
                            require(["viewmodels/createEncryptionConfirmation"], createEncryptionConfirmation => {
                                var createEncryptionConfirmationViewModel = new createEncryptionConfirmation(savedKey);
                                createEncryptionConfirmationViewModel.dialogPromise.done(() => encryptionConfirmationDialogPromise.resolve());
                                createEncryptionConfirmationViewModel.dialogPromise.fail(() => encryptionConfirmationDialogPromise.reject());
                                app.showDialog(createEncryptionConfirmationViewModel);
                            });
                        } else {
                            encryptionConfirmationDialogPromise.resolve();
                        }

                        this.createDefaultSettings(newDatabase, bundles).always(() => {
                            if (bundles.contains("Quotas") || bundles.contains("Versioning") || bundles.contains("SqlReplication")) {
                                encryptionConfirmationDialogPromise.always(() => {
                                    require(["viewmodels/databaseSettingsDialog"], databaseSettingsDialog => {
                                        var settingsDialog = new databaseSettingsDialog(bundles);
                                        app.showDialog(settingsDialog);
                                    });
                                });
                            }
                        });
                    });
            });
        });
    }

    private addNewDatabase(databaseName: string): database {
        var foundDatabase = this.databases.first((db: database) => db.name == databaseName);

        if (!foundDatabase) {
            var newDatabase = new database(databaseName);
            this.databases.unshift(newDatabase);
            return newDatabase;
        }

        return foundDatabase;
    }

    private createDefaultSettings(db: database, bundles: Array<string>): JQueryPromise<any> {
        var deferred = $.Deferred();
        require(["commands/createDefaultSettingsCommand"], createDefaultSettingsCommand => {
            deferred = new createDefaultSettingsCommand(db, bundles).execute();
        });
        return deferred;
    }

    private isEmptyStringOrWhitespace(str: string) {
        return !$.trim(str);
    }

    private getEncryptionAlgorithmFullName(encrytion: string) {
        var fullEncryptionName: string = null;
        switch (encrytion) {
            case "DES":
                fullEncryptionName = "System.Security.Cryptography.DESCryptoServiceProvider, mscorlib";
                break;
            case "R2C2":
                fullEncryptionName = "System.Security.Cryptography.RC2CryptoServiceProvider, mscorlib";
                break;
            case "Rijndael":
                fullEncryptionName = "System.Security.Cryptography.RijndaelManaged, mscorlib";
                break;
            default: //case "Triple DESC":
                fullEncryptionName = "System.Security.Cryptography.TripleDESCryptoServiceProvider, mscorlib";
        }
        return fullEncryptionName;
    }

    deleteSelectedDatabases(databases: Array<database>) {
        if (databases.length > 0) {
            require(["viewmodels/deleteResourceConfirm"], deleteDatabaseConfirm => {
                var confirmDeleteViewModel = new deleteDatabaseConfirm(databases);

                confirmDeleteViewModel.deleteTask.done((deletedDatabaseNames: string[]) => {
                    if (databases.length == 1) {
                        this.onDatabaseDeleted(databases[0].name);
                    } else {
                        deletedDatabaseNames.forEach(databaseName => {
                            this.onDatabaseDeleted(databaseName);
                        });
                    }
                });

                app.showDialog(confirmDeleteViewModel);
            });
        }
    }

    deleteCheckedDatabases() {
        var checkedDatabases: database[] = this.databases().filter((db: database) => db.isChecked());
        this.deleteSelectedDatabases(checkedDatabases);
    }

    private onDatabaseDeleted(databaseName: string) {
        var databaseInArray = this.databases.first((db: database) => db.name == databaseName);

        if (!!databaseInArray) {
            this.databases.remove(databaseInArray);

            if ((this.databases().length > 0) && (this.databases.contains(this.selectedDatabase()) === false)) {
                this.selectDatabase(this.databases().first());
            }
        }
    }

    toggleSelectedDatabases(databases: Array<database>) {
        if (databases.length > 0) {
            var action = !databases[0].disabled();

            require(["viewmodels/disableResourceToggleConfirm"], disableDatabaseToggleConfirm => {
                var disableDatabaseToggleViewModel = new disableDatabaseToggleConfirm(databases);

                disableDatabaseToggleViewModel.disableToggleTask
                    .done((toggledDatabasesNames: string[]) => {
                        var activeDatabase: database = this.activeDatabase();

                        if (databases.length == 1) {
                            this.onDatabaseDisabledToggle(databases[0].name, action, activeDatabase);
                        } else {
                            toggledDatabasesNames.forEach(databaseName => {
                                this.onDatabaseDisabledToggle(databaseName, action, activeDatabase);
                            });
                        }
                    });

                app.showDialog(disableDatabaseToggleViewModel);
            });
        }
    }

    toggleCheckedDatabases() {
        var checkedDatabases: database[] = this.databases().filter((db: database) => db.isChecked());
        this.toggleSelectedDatabases(checkedDatabases);
    }

    private onDatabaseDisabledToggle(databaseName: string, action: boolean, activeDatabase: database) {
        var db = this.databases.first((foundDb: database) => foundDb.name == databaseName);

        if (!!db) {
            db.disabled(action);
            db.isChecked(false);

            if (!!activeDatabase && db.name == activeDatabase.name) {
                this.selectDatabase(db);
            }
        }
    }

    private filterDatabases(filter: string) {
        var filterLower = filter.toLowerCase();
        this.databases().forEach((db: database) => {
            var isMatch = (!filter || (db.name.toLowerCase().indexOf(filterLower) >= 0)) && db.name != '<system>';
            db.isVisible(isMatch);
        });

        this.databases().map((db: database) => db.isChecked(!db.isVisible() ? false : db.isChecked()));
    }
}

export = databases;