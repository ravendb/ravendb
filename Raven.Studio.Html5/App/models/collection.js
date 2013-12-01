define(["require", "exports", "common/pagedList"], function(require, exports, __pagedList__) {
    
    var pagedList = __pagedList__;

    var collection = (function () {
        function collection(name, isAllCollections) {
            this.name = name;
            this.isAllCollections = isAllCollections;
            this.colorClass = "";
            this.documentCount = ko.observable(0);
        }
        // Notifies consumers that this collection should be the selected one.
        // Called from the UI when a user clicks a collection the documents page.
        collection.prototype.activate = function () {
            ko.postbox.publish("ActivateCollection", this);
        };
        return collection;
    })();

    
    return collection;
});
//# sourceMappingURL=collection.js.map
