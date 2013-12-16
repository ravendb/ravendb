import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router");

import appUrl = require("common/appUrl");
import raven = require("common/raven");
import database = require("models/database");
import createDatabase = require("viewmodels/createDatabase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");

class databases {

    ravenDb: raven;
    databases = ko.observableArray<database>();

    constructor() {
        this.ravenDb = new raven();
    }

    activate(navigationArgs) {
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

        var systemDatabase = new database("<system>");
        systemDatabase.isSystem = true;

        this.databases(results.concat(systemDatabase));

        // If we have just a few databases, grab the db stats for all of them.
        // (Otherwise, we'll grab them when we click them.)
        var few = 20;
        if (results.length < 20) {
            results.forEach(db => this.fetchStats(db));
        }
    }

    newDatabase() {
        var createDatabaseViewModel = new createDatabase();
        createDatabaseViewModel
            .creationTask
            .done((databaseName: string) => this.databases.unshift(new database(databaseName)));
        app.showDialog(createDatabaseViewModel);
    }

    fetchStats(db: database) {
        new getDatabaseStatsCommand(db)
            .execute()
            .done(result => db.statistics(result));
    }

    selectDatabase(db: database) {
        this.databases().forEach(d => d.isSelected(d === db));
        db.activate();
    }

    goToDocuments(db: database) {
        // TODO: use appUrl for this.
        router.navigate("#documents?database=" + db.name);
    }
}

export = databases;