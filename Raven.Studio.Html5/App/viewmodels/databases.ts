import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/database");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteDatabaseConfirm = require("viewmodels/deleteDatabaseConfirm");
import createDatabase = require("viewmodels/createDatabase");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import createEncryption = require("viewmodels/createEncryption");
import createEncryptionConfirmation = require("viewmodels/createEncryptionConfirmation");

class databases extends viewModelBase {

    databases = ko.observableArray<database>();
    searchText = ko.observable("");
    selectedDatabase = ko.observable<database>();
    systemDb: database;
    initializedStats: boolean;
    docsForSystemUrl: string;

    constructor() {
        super();

        this.systemDb = appUrl.getSystemDatabase();
        this.docsForSystemUrl = appUrl.forDocuments(null, this.systemDb);
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterDatabases(s));
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    attached() {
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
    }

    modelPolling() {
        new getDatabasesCommand()
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
        var databasesHaveChanged = this.checkDifferentDatabases(results);
        if (databasesHaveChanged) {            
            this.databases(results);

            // If we have just a few databases, grab the db stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < few && !this.initializedStats) {
                this.initializedStats = true;
                results.forEach(db => this.fetchStats(db));
            }

            // If we have no databases, show the "create a new database" screen.
            if (results.length === 0) {
                this.newDatabase();
            }
        }
    }

    checkDifferentDatabases(dbs: database[]) {
        if (dbs.length !== this.databases().length) {
            return true;
        }

        var freshDbNames = dbs.map(db => db.name);
        var existingDbNames = this.databases().map(d => d.name);
        return existingDbNames.some(existing => !freshDbNames.contains(existing));
    }

    newDatabase() {
        // Why do an inline require here? Performance.
        // Since the database page is the common landing page, we want it to load quickly.
        // Since the createDatabase page isn't required up front, we pull it in on demand.
        require(["viewmodels/createDatabase"], createDatabase => {
            var createDatabaseViewModel: createDatabase = new createDatabase(this.databases);
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
        var deferred = $.Deferred();
        var savedKey;

        if (bundles.contains("Encryption")) {
            var createEncryptionViewModel: createEncryption = new createEncryption();
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
                    deferred.resolve(securedSettings);
                });
            app.showDialog(createEncryptionViewModel);
        } else {
            deferred.resolve({});
        }

        deferred.done(() => {
            //var self = this; // Note: no reason to use self. If you want to use 'this' in a callback, put the callback in a lambda, e.g. .done(() => this.Foobar())
            new createDatabaseCommand(databaseName, settings, securedSettings)
                .execute()
                .done(() => {
                    this.databases.unshift(new database(databaseName));
                    if (!jQuery.isEmptyObject(securedSettings)) {
                        var createEncryptionConfirmationViewModel: createEncryptionConfirmation = new createEncryptionConfirmation(savedKey);
                        app.showDialog(createEncryptionConfirmationViewModel);
                    }
                });
        });
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

    fetchStats(db: database) {
        new getDatabaseStatsCommand(db)
            .execute()
            .done(result=> db.statistics(result));
    }

    selectDatabase(db: database) {
        this.databases().forEach(d=> d.isSelected(d === db));
        db.activate();
        this.selectedDatabase(db);
    }

    goToDocuments(db: database) {
        // TODO: use appUrl for this.
        router.navigate("#documents?database=" + encodeURIComponent(db.name));
    }

    filterDatabases(filter: string) {
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

    deleteSelectedDatabase() {
        var db = this.selectedDatabase();
        if (db) {
            require(["viewmodels/deleteDatabaseConfirm"], deleteDatabaseConfirm => {
                var confirmDeleteVm: deleteDatabaseConfirm = new deleteDatabaseConfirm(db, this.systemDb);
                confirmDeleteVm.deleteTask.done(() => this.onDatabaseDeleted(db));
                app.showDialog(confirmDeleteVm);
            });
        }
    }

    onDatabaseDeleted(db: database) {
        this.databases.remove(db);
        this.databases.valueHasMutated();
        if (this.databases.length === 0)
            this.selectDatabase(this.systemDb);
        else if (this.databases.contains(this.selectedDatabase()) === false) {
            this.selectDatabase(this.databases().first());
        }
    }
}

export = databases; 