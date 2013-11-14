define(["require", "exports"], function(require, exports) {
    var database = (function () {
        function database(name) {
            var _this = this;
            this.name = name;
            this.isSystem = false;
            this.isSelected = ko.observable(false);
            this.statistics = ko.observable();
            this.docCount = ko.computed(function () {
                return _this.statistics() ? _this.statistics().CountOfDocuments : 0;
            });
        }
        database.prototype.activate = function () {
            ko.postbox.publish("ActivateDatabase", this);
        };
        return database;
    })();

    
    return database;
});
//# sourceMappingURL=database.js.map
