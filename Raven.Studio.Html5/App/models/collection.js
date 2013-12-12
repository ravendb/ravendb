define(["require", "exports", "common/pagedList", "commands/getCollectionInfoCommand", "models/collectionInfo", "models/database"], function(require, exports, pagedList, getCollectionInfoCommand, collectionInfo, database) {
    var collection = (function () {
        function collection(name) {
            this.name = name;
            this.colorClass = "";
            this.documentCount = ko.observable(0);
            this.isAllDocuments = false;
            this.isSystemDocuments = false;
        }
        // Notifies consumers that this collection should be the selected one.
        // Called from the UI when a user clicks a collection the documents page.
        collection.prototype.activate = function () {
            ko.postbox.publish("ActivateCollection", this);
        };

        collection.prototype.getInfo = function (db) {
            var _this = this;
            new getCollectionInfoCommand(this, db).execute().done(function (info) {
                return _this.documentCount(info.totalResults);
            });
        };
        return collection;
    })();

    
    return collection;
});
//# sourceMappingURL=collection.js.map
