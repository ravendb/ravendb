import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");

class createDatabase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;
    databaseName = ko.observable('');

    constructor() {
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // Next needs to configure bundle settings, if we've selected some bundles.
        // We haven't yet implemented bundle configuration, so for now we're just 
        // creating the database.
        var databaseName = this.databaseName();
        var createDbCommand = new createDatabaseCommand(databaseName);
        var createDbTask = createDbCommand.execute();
        createDbTask.done(() => this.creationTask.resolve(databaseName));
        createDbTask.fail(response => this.creationTask.reject(response));
        this.creationTaskStarted = true;
        dialog.close(this);
    }
}

export = createDatabase;