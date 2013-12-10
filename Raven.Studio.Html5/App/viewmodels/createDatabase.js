define(["require", "exports", "models/document", "plugins/dialog", "commands/createDatabaseCommand", "models/collection"], function(require, exports, document, dialog, createDatabaseCommand, collection) {
    var createDatabase = (function () {
        function createDatabase() {
            this.creationTask = $.Deferred();
            this.creationTaskStarted = false;
            this.databaseName = ko.observable('');
        }
        createDatabase.prototype.cancel = function () {
            dialog.close(this);
        };

        createDatabase.prototype.deactivate = function () {
            // If we were closed via X button or other dialog dismissal, reject the deletion task since
            // we never started it.
            if (!this.creationTaskStarted) {
                this.creationTask.reject();
            }
        };

        createDatabase.prototype.nextOrCreate = function () {
            var _this = this;
            // Next needs to configure bundle settings, if we've selected some bundles.
            // We haven't yet implemented bundle configuration, so for now we're just
            // creating the database.
            var databaseName = this.databaseName();
            var createDbCommand = new createDatabaseCommand(databaseName);
            var createDbTask = createDbCommand.execute();
            createDbTask.done(function () {
                return _this.creationTask.resolve(databaseName);
            });
            createDbTask.fail(function (response) {
                return _this.creationTask.reject(response);
            });
            this.creationTaskStarted = true;
            dialog.close(this);
        };
        return createDatabase;
    })();

    
    return createDatabase;
});
//# sourceMappingURL=createDatabase.js.map
