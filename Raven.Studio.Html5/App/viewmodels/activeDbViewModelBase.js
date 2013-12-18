define(["require", "exports", "common/appUrl"], function(require, exports, appUrl) {
    /*
    * Base view model class that keeps track of the currently selected database.
    */
    var activeDbViewModelBase = (function () {
        function activeDbViewModelBase() {
            this.activeDatabase = ko.observable().subscribeTo("ActivateDatabase", true);
        }
        activeDbViewModelBase.prototype.activate = function (args) {
            this.activeDatabase(appUrl.getDatabase());
        };

        activeDbViewModelBase.prototype.deactivate = function () {
            this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        };
        return activeDbViewModelBase;
    })();

    
    return activeDbViewModelBase;
});
//# sourceMappingURL=activeDbViewModelBase.js.map
