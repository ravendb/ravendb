define(["require", "exports", "models/database", "common/raven", "common/pagedList"], function(require, exports, __database__, __raven__, __pagedList__) {
    var database = __database__;
    var raven = __raven__;
    var pagedList = __pagedList__;

    // Helper class with static methods for generating app URLs.
    var appUrl = (function () {
        function appUrl() {
        }
        appUrl.forEditDoc = /**
        * Gets the URL for edit document.
        * @param id The ID of the document to edit, or null to edit a new document.
        * @param collectionName The name of the collection to page through on the edit document, or null if paging will be disabled.
        * @param docIndexInCollection The 0-based index of the doc to edit inside the paged collection, or null if paging will be disabled.
        * @param database The database to use in the URL. If null, the current database will be used.
        */
        function (id, collectionName, docIndexInCollection, db) {
            if (typeof db === "undefined") { db = raven.activeDatabase(); }
            var databaseUrlPart = appUrl.getEncodedDbPart(db);
            var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
            var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
            return "#edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
        };

        appUrl.forStatus = /**
        * Gets the URL for status page.
        * @param database The database to use in the URL. If null, the current database will be used.
        */
        function (db) {
            if (typeof db === "undefined") { db = raven.activeDatabase(); }
            return "#status?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forDocuments = function (collection, db) {
            if (typeof db === "undefined") { db = raven.activeDatabase(); }
            var databasePart = appUrl.getEncodedDbPart(db);
            var collectionPart = collection ? "&collection=" + encodeURIComponent(collection) : "";
            return "#documents?" + collectionPart + databasePart;
        };

        appUrl.forCurrentDatabase = /**
        * Gets an object containing computed URLs that update when the current database updates.
        */
        function () {
            return appUrl.currentDbComputeds;
        };

        appUrl.getEncodedDbPart = function (db) {
            return db ? "&database=" + encodeURIComponent(db.name) : "";
        };
        appUrl.currentDbComputeds = {
            documents: ko.computed(function () {
                return appUrl.forDocuments();
            }),
            status: ko.computed(function () {
                return appUrl.forStatus();
            })
        };
        return appUrl;
    })();

    
    return appUrl;
});
//# sourceMappingURL=appUrl.js.map
