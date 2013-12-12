define(["require", "exports", "plugins/router", "models/collection", "models/database", "models/document", "viewmodels/deleteCollection", "common/raven", "common/pagedList"], function(require, exports, router, collection, database, document, deleteCollection, raven, pagedList) {
    var userInfo = (function () {
        function userInfo() {
            this.displayName = "user info";
            this.data = ko.observable();
            this.ravenDb = new raven();
        }
        userInfo.prototype.activate = function (args) {
            var _this = this;
            console.log("this is USERINFO!");

            if (args && args.database) {
                ko.postbox.publish("ActivateDatabaseWithName", args.database);
            }

            this.ravenDb.userInfo().done(function (info) {
                _this.data(info);
            });
        };

        userInfo.prototype.canDeactivate = function () {
            return true;
        };
        return userInfo;
    })();

    
    return userInfo;
});
//# sourceMappingURL=userInfo.js.map
