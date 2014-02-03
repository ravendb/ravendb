var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/getUserInfoCommand", "common/appUrl", "models/database", "viewmodels/viewModelBase"], function(require, exports, getUserInfoCommand, appUrl, database, viewModelBase) {
    var userInfo = (function (_super) {
        __extends(userInfo, _super);
        function userInfo() {
            _super.apply(this, arguments);
            this.data = ko.observable();
        }
        userInfo.prototype.activate = function (args) {
            var _this = this;
            _super.prototype.activate.call(this, args);

            this.activeDatabase.subscribe(function () {
                return _this.fetchUserInfo();
            });
            return this.fetchUserInfo();
        };

        userInfo.prototype.fetchUserInfo = function () {
            var _this = this;
            var db = this.activeDatabase();
            if (db) {
                return new getUserInfoCommand(db).execute().done(function (results) {
                    return _this.data(results);
                });
            }

            return null;
        };
        return userInfo;
    })(viewModelBase);

    
    return userInfo;
});
//# sourceMappingURL=userInfo.js.map
