define(["require", "exports", "durandal/app", "plugins/router", "common/appUrl", "models/database", "viewmodels/createDatabase", "commands/getDatabaseStatsCommand", "commands/getDatabasesCommand", "viewmodels/deleteDatabaseConfirm"], function(require, exports, app, router, appUrl, database, createDatabase, getDatabaseStatsCommand, getDatabasesCommand, deleteDatabaseConfirm) {
    var databases = (function () {
        function databases() {
            var _this = this;
            this.databases = ko.observableArray();
            this.searchText = ko.observable("");
            this.selectedDatabase = ko.observable();
            this.searchText.subscribe(function (s) {
                return _this.filterDatabases(s);
            });
        }
        databases.prototype.activate = function (navigationArgs) {
            var _this = this;
            new getDatabasesCommand().execute().done(function (results) {
                return _this.databasesLoaded(results);
            });
        };

        databases.prototype.navigateToDocuments = function (db) {
            db.activate();
            router.navigate(appUrl.forDocuments(null, db));
        };

        databases.prototype.getDocumentsUrl = function (db) {
            return appUrl.forDocuments(null, db);
        };

        databases.prototype.databasesLoaded = function (results) {
            var _this = this;
            var systemDatabase = new database("<system>");
            systemDatabase.isSystem = true;

            this.databases(results.concat(systemDatabase));

            // If we have just a few databases, grab the db stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < 20) {
                results.forEach(function (db) {
                    return _this.fetchStats(db);
                });
            }

            // If we have no databases, show the "create a new database" screen.
            if (results.length === 0) {
                this.newDatabase();
            }
        };

        databases.prototype.newDatabase = function () {
            var _this = this;
            var createDatabaseViewModel = new createDatabase();
            createDatabaseViewModel.creationTask.done(function (databaseName) {
                return _this.databases.unshift(new database(databaseName));
            });
            app.showDialog(createDatabaseViewModel);
        };

        databases.prototype.fetchStats = function (db) {
            new getDatabaseStatsCommand(db).execute().done(function (result) {
                return db.statistics(result);
            });
        };

        databases.prototype.selectDatabase = function (db) {
            this.databases().forEach(function (d) {
                return d.isSelected(d === db);
            });
            db.activate();
            this.selectedDatabase(db);
        };

        databases.prototype.goToDocuments = function (db) {
            // TODO: use appUrl for this.
            router.navigate("#documents?database=" + db.name);
        };

        databases.prototype.filterDatabases = function (filter) {
            var filterLower = filter.toLowerCase();
            this.databases().forEach(function (d) {
                var isMatch = !filter || d.name.toLowerCase().indexOf(filterLower) >= 0;
                d.isVisible(isMatch);
            });
        };

        databases.prototype.deleteSelectedDatabase = function () {
            var _this = this;
            var db = this.selectedDatabase();
            var systemDb = this.databases.first(function (db) {
                return db.isSystem;
            });
            if (db && systemDb) {
                var confirmDeleteVm = new deleteDatabaseConfirm(db, systemDb);
                confirmDeleteVm.deleteTask.done(function () {
                    return _this.onDatabaseDeleted(db);
                });
                app.showDialog(confirmDeleteVm);
            }
        };

        databases.prototype.onDatabaseDeleted = function (db) {
            this.databases.remove(db);
            if (this.selectedDatabase() === db) {
                this.selectedDatabase(null);
            }
        };
        return databases;
    })();

    
    return databases;
});
//# sourceMappingURL=databases.js.map
