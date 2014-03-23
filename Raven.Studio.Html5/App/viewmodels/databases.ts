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
    lastModelPayloadHash: number;

    constructor() {
        super();
        this.searchText.subscribe(s=> this.filterDatabases(s));
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

    initializedStats: boolean;

    databasesLoaded(results: Array<database>) {

        var modelPayloadHash = results.map(d => d.name).join().hashCode();
        var databasesHaveChanged = !this.lastModelPayloadHash || this.lastModelPayloadHash !== modelPayloadHash;
        if (databasesHaveChanged) {
            this.lastModelPayloadHash = modelPayloadHash;

            var systemDatabase = new database("<system>");
            systemDatabase.isSystem = true;

            this.databases(results.concat(systemDatabase));

            // If we have just a few databases, grab the db stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < 20 && !this.initializedStats) {
                this.initializedStats = true;
                results.forEach(db=> this.fetchStats(db));
            }

            /*// If we have no databases, show the "create a new database" screen.
            if (results.length === 0) {
                this.newDatabase();
            }*/
        }
    }

    newDatabase() {
        require(["viewmodels/createDatabase"], createDatabase => {
            var createDatabaseViewModel: createDatabase = new createDatabase(this.databases);
            createDatabaseViewModel
                .creationTask
                .done((databaseName: string, bundles: string[]) => {
                    var securedSettings = {};
                    var deffered = $.Deferred();
                    var savedKey;

                    if (bundles.indexOf("Encryption") != -1) {
                        var createEncryptionViewModel: createEncryption = new createEncryption();
                        createEncryptionViewModel
                            .creationEncryption
                            .done((key: string, encryptionAlgorithm: string, isEncryptedIndexes: string) => {
                                savedKey = key;
                                securedSettings = {
                                    'Raven/Encryption/Key': key,
                                    'Raven/Encryptijon/Algorithm': this.getEncryptionAlgorithmFullName(encryptionAlgorithm),
                                    'Raven/Encryption/EncryptIndexes': isEncryptedIndexes
                                };
                                deffered.resolve(securedSettings);
                            });
                        app.showDialog(createEncryptionViewModel);
                    } else {
                        deffered.resolve({});
                    }

                    deffered.done(() => {
                        this.createDB(databaseName, bundles, securedSettings)
                            .done(() => {
                                this.databases.unshift(new database(databaseName));
                                if (!jQuery.isEmptyObject(securedSettings)) {
                                    var createEncryptionConfirmationViewModel: createEncryptionConfirmation = new createEncryptionConfirmation(savedKey);
                                    app.showDialog(createEncryptionConfirmationViewModel);
                                }
                        });
                    });
                });
            app.showDialog(createDatabaseViewModel);
        });
    }

    private createDB(databaseName: string, bundles: string[], securedSettings: {}) {
        var self = this;
        return new createDatabaseCommand(databaseName, bundles, securedSettings)
            .execute()
            .fail(response=> {
                //self.creationTask.reject(response);
            })
            .done(result=> {
                //self.creationTask.resolve(databaseName);
                //dialog.close(self);
            });
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
        router.navigate("#documents?database=" + db.name);
    }

    filterDatabases(filter: string) {
        var filterLower = filter.toLowerCase();
        this.databases().forEach(d=> {
            var isMatch = !filter || (d.name.toLowerCase().indexOf(filterLower) >= 0);
            d.isVisible(isMatch);
        });
    }

    deleteSelectedDatabase() {
        var db = this.selectedDatabase();
        var systemDb = this.databases.first(db=> db.isSystem);
        if (db && systemDb) {
            require(["viewmodels/deleteDatabaseConfirm"], deleteDatabaseConfirm => {
                var confirmDeleteVm: deleteDatabaseConfirm = new deleteDatabaseConfirm(db, systemDb);
                confirmDeleteVm.deleteTask.done(() => this.onDatabaseDeleted(db));
                app.showDialog(confirmDeleteVm);
            });
        }
    }

    onDatabaseDeleted(db: database) {
        this.databases.remove(db);
        if (this.selectedDatabase() === db) {
            this.selectDatabase(this.databases().first());
        }
    }
}

export = databases; 