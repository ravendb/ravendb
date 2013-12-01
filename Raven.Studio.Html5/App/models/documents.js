define(["require", "exports", "models/database", "models/collection", "common/raven", "common/pagedList"], function(require, exports, __database__, __collection__, __raven__, __pagedList__) {
    
    
    
    

    var database = __database__;
    var collection = __collection__;
    

    var raven = __raven__;
    var pagedList = __pagedList__;

    var documents = (function () {
        function documents() {
            var _this = this;
            this.displayName = "documents";
            this.collections = ko.observableArray();
            this.selectedCollection = ko.observable().subscribeTo("ActivateCollection");
            this.collectionColors = [];
            this.collectionsLoadedTask = $.Deferred();
            this.collectionDocumentsLoaded = 0;
            this.currentCollectionPagedItems = ko.observable();
            this.ravenDb = new raven();
            this.ravenDb.collections().then(function (results) {
                return _this.collectionsLoaded(results);
            });

            this.selectedCollection.subscribe(function (c) {
                return _this.onSelectedCollectionChanged(c);
            });
        }
        documents.prototype.collectionsLoaded = function (collections) {
            var _this = this;
            // Set the color class for each of the collections.
            // These styles are found in app.less.
            var collectionStyleCount = 15;
            collections.forEach(function (c, index) {
                return c.colorClass = "collection-style-" + (index % collectionStyleCount);
            });

            // Create the "All Documents" pseudo collection.
            this.allDocumentsCollection = new collection("All Documents");
            this.allDocumentsCollection.colorClass = "all-documents-collection";
            this.allDocumentsCollection.documentCount = ko.computed(function () {
                return _this.collections().filter(function (c) {
                    return c !== _this.allDocumentsCollection;
                }).map(function (c) {
                    return c.documentCount();
                }).reduce(function (first, second) {
                    return first + second;
                }, 0);
            });

            // All systems a-go. Load them into the UI and select the first one.
            var allCollections = [this.allDocumentsCollection].concat(collections);
            this.collections(allCollections);

            var collectionToSelect = collections.filter(function (c) {
                return c.name === _this.collectionToSelectName;
            })[0] || this.allDocumentsCollection;
            collectionToSelect.activate();

            // Fetch the collection info for each collection.
            // The collection info contains information such as total number of documents.
            collections.forEach(function (c) {
                return _this.fetchTotalDocuments(c);
            });
        };

        documents.prototype.fetchTotalDocuments = function (collection) {
            var _this = this;
            this.ravenDb.collectionInfo(collection.name).then(function (info) {
                collection.documentCount(info.totalResults);
                _this.collectionDocumentsLoaded++;
                if (_this.collectionDocumentsLoaded === _this.collections().length - 1) {
                    _this.collectionsLoadedTask.resolve();
                }
            });
        };

        documents.prototype.onSelectedCollectionChanged = function (selected) {
            var _this = this;
            if (collection) {
                var fetcher = function (skip, take) {
                    var collectionName = selected !== _this.allDocumentsCollection ? selected.name : null;
                    return _this.ravenDb.documents(collectionName, skip, take);
                };

                var documentsList = new pagedList(fetcher, 30);
                this.currentCollectionPagedItems(documentsList);
            }
        };

        documents.prototype.activate = function (args) {
            // Initialize the context menu (using Bootstrap-ContextMenu library).
            // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
            ($('.document-collections')).contextmenu({ 'target': '#collections-context-menu' });

            // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo/123
            this.collectionToSelectName = args ? args.collection : null;
            console.log("zzzz", args);
            return this.collectionsLoadedTask;
        };
        return documents;
    })();

    
    return documents;
});
//# sourceMappingURL=documents.js.map
