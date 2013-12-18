define(["require", "exports", "commands/getUserInfoCommand", "common/appUrl"], function(require, exports, getUserInfoCommand, appUrl) {
    var userInfo = (function () {
        function userInfo() {
            var _this = this;
            this.data = ko.observable();
            this.activeDbSubscription = ko.postbox.subscribe("ActivateDatabase", function (db) {
                return _this.fetchUserInfo(db);
            });
        }
        userInfo.prototype.activate = function (args) {
            var db = appUrl.getDatabase();
            this.fetchUserInfo(db);
        };

        userInfo.prototype.deactivate = function () {
            this.activeDbSubscription.dispose();
        };

        userInfo.prototype.fetchUserInfo = function (db) {
            var _this = this;
            if (db) {
                return new getUserInfoCommand(db).execute().done(function (results) {
                    return _this.data(results);
                });
            }
        };
        return userInfo;
    })();

    
    return userInfo;
});
//# sourceMappingURL=userInfo.js.map
