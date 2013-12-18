define(["require", "exports", "common/appUrl"], function(require, exports, appUrl) {
    /*
    * Base view model class that keeps track of the currently selected database.
    */
    var viewModelBase = (function () {
        function viewModelBase() {
            this.activeDatabase = ko.observable().subscribeTo("ActivateDatabase", true);
        }
        viewModelBase.prototype.activate = function (args) {
            this.activeDatabase(appUrl.getDatabase());
        };

        viewModelBase.prototype.deactivate = function () {
            this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        };
        return viewModelBase;
    })();

    
    return viewModelBase;
});
//# sourceMappingURL=viewModelBase.js.map
