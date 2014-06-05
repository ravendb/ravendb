import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/database");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import createDefaultSettingsCommand = require("commands/createDefaultSettingsCommand");
import createEncryption = require("viewmodels/createEncryption");
import createEncryptionConfirmation = require("viewmodels/createEncryptionConfirmation");
import changesApi = require('common/changesApi');
import shell = require('viewmodels/shell');
import changeSubscription = require('models/changeSubscription');
import databaseSettingsDialog = require("viewmodels/databaseSettingsDialog");

class databases extends viewModelBase {

    databases = ko.observableArray<database>();
    searchText = ko.observable("");
    selectedDatabase = ko.observable<database>();
    systemDb: database;
    initializedStats: boolean;
    docsForSystemUrl: string;
    databasesChangeSubscription: changeSubscription;
    isFirstLoad = true;

    constructor() {
        super();

        this.systemDb = appUrl.getSystemDatabase();
        this.docsForSystemUrl = appUrl.forDocuments(null, this.systemDb);
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterDatabases(s));
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        var result = $.Deferred();

        this.fetchDatabases().always(() => result.resolve({ can: true }));
        
        return result;
    }

    attached() {
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
        this.databasesChangeSubscription = shell.globalChangesApi.watchDocPrefix((e: documentChangeNotificationDto) => this.changesApiFiredForDatabases(e), "Raven/Databases");
    }

    deactivate() {
        super.deactivate();
        this.databasesChangeSubscription.off();
    }

    fetchDatabases(): JQueryPromise<database[]> {
        return new getDatabasesCommand()
            .execute()
            .done((results: database[]) => this.databasesLoaded(results));
    }

    navigateToDocuments(db: database) {
        db.activate();
        router.navigate(appUrl.forDocuments(null, db));
    }

    getDocumentsUrl(db: database) {
        return appUrl.forDocuments(null, db);
    }

    databasesLoaded(results: Array<database>) {
        // If we have no databases, show the "create a new database" screen.
        if (results.length === 0 && this.isFirstLoad) {
            this.newDatabase();
        }

        var databasesHaveChanged = this.checkDifferentDatabases(results);
        if (databasesHaveChanged) {            
            this.databases(results);

            // If we have just a few databases, grab the db stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < few && !this.initializedStats) {
                this.initializedStats = true;

                for (var i = 0; i < results.length; i++) {
                    var db: database = results[i];
                    if (!db.disabled()) {
                        this.fetchStats(db);
                    }
                }
            }
        }

        this.isFirstLoad = false;
    }

    private checkDifferentDatabases(newDatabases: database[]) {
        if (newDatabases.length !== this.databases().length) {
            return true;
        }

        var existingDbs = this.databases();
        return existingDbs.some(existingDb => !this.containsObject(newDatabases, existingDb));
    }

    private containsObject(dbs: database[], db: database) {
        for (var i = 0; i < dbs.length; i++) {
            if (dbs[i].name == db.name && dbs[i].disabled() == db.disabled()) {
                return true;
            }
        }
        return false;
    }

    newDatabase() {
        // Why do an inline require here? Performance.
        // Since the database page is the common landing page, we want it to load quickly.
        // Since the createDatabase page isn't required up front, we pull it in on demand.
        require(["viewmodels/createDatabase"], createDatabase => {
            var createDatabaseViewModel = new createDatabase(this.databases);
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
        var settingsDeferred = $.Deferred();

        if (bundles.contains("Encryption")) {
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
        } else {
            encryptionDeferred.resolve();
        }

        encryptionDeferred.done((encryptionResult) => {
            new createDatabaseCommand(databaseName, settings, securedSettings)
                .execute()
                .done(() => {
                    var newDb = new database(databaseName);
                    this.addNewDatabase(newDb);

                    var encryptionConfirmationDialogPromise = $.Deferred();
                    if (!jQuery.isEmptyObject(securedSettings)) {
                        var createEncryptionConfirmationViewModel: createEncryptionConfirmation = new createEncryptionConfirmation(savedKey);
                        createEncryptionConfirmationViewModel.dialogPromise.done(() => encryptionConfirmationDialogPromise.resolve());
                        createEncryptionConfirmationViewModel.dialogPromise.fail(() => encryptionConfirmationDialogPromise.reject());
                        app.showDialog(createEncryptionConfirmationViewModel);
                    } else {
                        encryptionConfirmationDialogPromise.resolve();
                    }

                    this.selectDatabase(newDb);

                    this.createDefaultSettings(newDb, bundles).always(() => {
                      if (bundles.contains("Quotas") || bundles.contains("Versioning")) {
                        encryptionConfirmationDialogPromise.always(() => {
                          var settingsDialog = new databaseSettingsDialog(bundles);
                          app.showDialog(settingsDialog);
                        });
                      }
                    });
                });
        });
    }

    private createDefaultSettings(db: database, bundles: Array<string>): JQueryPromise<any> {
        return new createDefaultSettingsCommand(db, bundles).execute();
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

    private fetchStats(db: database) {
        new getDatabaseStatsCommand(db)
            .execute()
            .done(result=> db.statistics(result));
    }

    selectDatabase(db: database) {
        this.databases().forEach(d=> d.isSelected(d.name === db.name));
        db.activate();
        this.selectedDatabase(db);
    }

    goToDocuments(db: database) {
        router.navigate(appUrl.forDocuments(null, db));
    }

    deleteSelectedDatabase() {
        var db = this.selectedDatabase();
        if (db) {
            require(["viewmodels/deleteDatabaseConfirm"], deleteDatabaseConfirm => {
                var confirmDeleteViewModel = new deleteDatabaseConfirm(db, this.systemDb);
                confirmDeleteViewModel.deleteTask.done(()=> {
                    this.onDatabaseDeleted(db.name);
                });
                app.showDialog(confirmDeleteViewModel);
            });
        }
    }

    toggleSelectedDatabase() {
        var db = this.selectedDatabase();
        if (db) {
            var desiredAction = db.disabled() ? "enable" : "disable";
            var desiredActionCapitalized = desiredAction.charAt(0).toUpperCase() + desiredAction.slice(1);

            var confirmationMessageViewModel = this.confirmationMessage(desiredActionCapitalized + ' Database', 'Are you sure you want to ' + desiredAction + ' the database?');
            confirmationMessageViewModel
                .done(() => {
                    if (shell.currentDbChangesApi()) {
                        shell.currentDbChangesApi().dispose();
                    }
                    require(["commands/toggleDatabaseDisabledCommand"], toggleDatabaseDisabledCommand => {
                        new toggleDatabaseDisabledCommand(db)
                            .execute()
                            .done(() => {
                                db.isSelected(false);
                                db.disabled(!db.disabled());
                                this.selectDatabase(db);
                            });
                    });
                });
        }
    }

    private filterDatabases(filter: string) {
        var filterLower = filter.toLowerCase();
        this.databases().forEach(d=> {
            var isMatch = !filter || (d.name.toLowerCase().indexOf(filterLower) >= 0);
            d.isVisible(isMatch);
        });

        var selectedDatabase = this.selectedDatabase();
        if (selectedDatabase && !selectedDatabase.isVisible()) {
            selectedDatabase.isSelected(false);
            this.selectedDatabase(null);
        }
    }

    private onDatabaseDeleted(databaseName: string) {
        var databaseInList = this.databases.first((curDB: database) => curDB.name == databaseName);
        if (!!databaseInList) {
            this.databases.remove(databaseInList);
        }
        if (this.databases().length === 0)
            this.selectDatabase(this.systemDb);
        else if (this.databases.contains(this.selectedDatabase()) === false) {
            this.selectDatabase(this.databases().first());
        }
    }

    private addNewDatabase(db: database) {
        var databaseInList = this.databases.first((curDb: database) => curDb.name == db.name);
        if (!databaseInList) {
            this.databases.unshift(db);
        }
    }

    private changesApiFiredForDatabases(e: documentChangeNotificationDto) {
        if (!!e.Id && e.Id.indexOf("Raven/Databases") === 0 &&
            (e.Type === documentChangeType.Put || e.Type === documentChangeType.Delete)) {
            if (e.Type === documentChangeType.Delete) {
                var receivedDocumentName = e.Id.slice(e.Id.lastIndexOf('/') + 1);
                this.onDatabaseDeleted(receivedDocumentName);
            }
            if (e.Type === documentChangeType.Put) {
                var receivedDocumentName = e.Id.slice(e.Id.lastIndexOf('/') + 1);
                this.addNewDatabase(new database(receivedDocumentName));
            }
        }
    }
}

export = databases; 