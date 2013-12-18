define(["require", "exports", "commands/getUserInfoCommand", "common/appUrl", "models/database"], function(require, exports, getUserInfoCommand, appUrl, database) {
    var userInfo = (function () {
        function userInfo() {
            this.data = ko.observable();
        }
        userInfo.prototype.activate = function (args) {
            var _this = this;
            var db = appUrl.getDatabase();
            return new getUserInfoCommand(db).execute().done(function (results) {
                return _this.data(results);
            });
        };
        return userInfo;
    })();

    
    return userInfo;
});
//# sourceMappingURL=userInfo.js.map
