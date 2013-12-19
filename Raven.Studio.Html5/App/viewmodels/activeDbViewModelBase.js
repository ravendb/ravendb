define(["require", "exports", "common/appUrl", "models/database"], function(require, exports, appUrl, database) {
    /*
    * Base view model class that keeps track of the currently selected database.
    */
    var activeDbViewModelBase = (function () {
        function activeDbViewModelBase() {
            this.activeDatabase = ko.observable().subscribeTo("ActivateDatabase", true);
        }
        activeDbViewModelBase.prototype.activate = function (args) {
            var db = appUrl.getDatabase();
            var currentDb = this.activeDatabase();
            if (!currentDb || currentDb.name !== db.name) {
                ko.postbox.publish("ActivateDatabaseWithName", db.name);
            }
        };

        activeDbViewModelBase.prototype.deactivate = function () {
            this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        };
        return activeDbViewModelBase;
    })();

    
    return activeDbViewModelBase;
});
//# sourceMappingURL=activeDbViewModelBase.js.map
