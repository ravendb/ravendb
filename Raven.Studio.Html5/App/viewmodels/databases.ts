import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router");

import raven = require("common/raven");
import database = require("models/database");
import createDatabase = require("viewmodels/createDatabase");

class databases {

    ravenDb: raven;
    databases = ko.observableArray<database>();

    constructor() {
        this.ravenDb = new raven();
    }

    activate(navigationArgs) {
        this.ravenDb
            .databases()
            .done((results: Array<database>) => this.databasesLoaded(results));
    }

    navigateToDocuments(db: database) {
        db.activate();
        router.navigate("#documents?db=" + encodeURIComponent(db.name));
    }

    databasesLoaded(results: Array<database>) {
        this.databases(results);

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
            .done((databaseName: string) => this.databases.push(new database(databaseName)));
        app.showDialog(createDatabaseViewModel);
    }

    fetchStats(db: database) {
        return this.ravenDb
            .databaseStats(db.name)
            .done(result => db.statistics(result));
    }

    selectDatabase(db: database) {
        this.databases().forEach(d => d.isSelected(d === db));
        db.activate();
    }

    goToDocuments(db: database) {
        router.navigate("#documents?database=" + db.name);
    }
}

export = databases;