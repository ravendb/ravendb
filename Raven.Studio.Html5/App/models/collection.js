define(["require", "exports", "common/pagedList", "commands/getCollectionInfoCommand", "commands/getDocumentsCommand", "commands/getSystemDocumentsCommand", "models/collectionInfo", "common/pagedResultSet", "models/database"], function(require, exports, pagedList, getCollectionInfoCommand, getDocumentsCommand, getSystemDocumentsCommand, collectionInfo, pagedResultSet, database) {
    var collection = (function () {
        function collection(name, ownerDatabase) {
            this.name = name;
            this.ownerDatabase = ownerDatabase;
            this.colorClass = "";
            this.documentCount = ko.observable(0);
            this.isAllDocuments = false;
            this.isSystemDocuments = false;
            this.isAllDocuments = name === collection.allDocsCollectionName;
            this.isSystemDocuments = name === collection.systemDocsCollectionName;
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

        collection.prototype.getDocuments = function () {
            if (!this.documentsList) {
                this.documentsList = this.createPagedList();
            }

            return this.documentsList;
        };

        collection.prototype.fetchDocuments = function (skip, take) {
            if (this.isSystemDocuments) {
                return new getSystemDocumentsCommand(this.ownerDatabase, skip, take).execute();
            } else {
                return new getDocumentsCommand(this, skip, take).execute();
            }
        };

        collection.createSystemDocsCollection = function (ownerDatabase) {
            return new collection(collection.systemDocsCollectionName, ownerDatabase);
        };

        collection.createAllDocsCollection = function (ownerDatabase) {
            return new collection(collection.allDocsCollectionName, ownerDatabase);
        };

        collection.prototype.createPagedList = function () {
            var _this = this;
            var fetcher = function (skip, take) {
                return _this.fetchDocuments(skip, take);
            };
            var list = new pagedList(fetcher);
            list.collectionName = this.name;
            return list;
        };
        collection.allDocsCollectionName = "All Documents";
        collection.systemDocsCollectionName = "System Documents";
        return collection;
    })();

    
    return collection;
});
//# sourceMappingURL=collection.js.map
