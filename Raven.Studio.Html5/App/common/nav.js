define(["require", "exports", "common/raven"], function(require, exports, __raven__) {
    
    var raven = __raven__;

    // Helper class with static methods for generating app URLs.
    var nav = (function () {
        function nav() {
        }
        nav.editDocument = // Gets the URL for edit document.
        function (id, collectionName, docIndexInCollection, db) {
            if (typeof db === "undefined") { db = raven.activeDatabase(); }
            var databaseUrlPart = db ? "&database=" + encodeURIComponent(db.name) : "";
            var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) + databaseUrlPart : "";
            var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
            return "edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
        };
        return nav;
    })();

    
    return nav;
});
//# sourceMappingURL=nav.js.map
