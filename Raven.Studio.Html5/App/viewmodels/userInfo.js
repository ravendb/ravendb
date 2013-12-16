define(["require", "exports", "models/database", "common/raven"], function(require, exports, database, raven) {
    var userInfo = (function () {
        function userInfo() {
            this.displayName = "user info";
            this.data = ko.observable();
            this.ravenDb = new raven();
        }
        userInfo.prototype.activate = function (args) {
            var _this = this;
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
