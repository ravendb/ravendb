define(["require", "exports", "durandal/app", "plugins/router", "models/collection", "models/database", "models/document", "viewModels/deleteCollection", "common/raven", "common/pagedList", "common/appUrl", "commands/getDocumentsCommand"], function(require, exports, __app__, __router__, __collection__, __database__, __document__, __deleteCollection__, __raven__, __pagedList__, __appUrl__, __getDocumentsCommand__) {
    var app = __app__;
    var router = __router__;

    var collection = __collection__;
    var database = __database__;
    var document = __document__;
    var deleteCollection = "viewModels/deleteCollection";
    var raven = __raven__;
    var pagedList = __pagedList__;
    var appUrl = __appUrl__;
    var getDocumentsCommand = __getDocumentsCommand__;

    var documents = (function () {
        function documents() {
            var _this = this;
            this.displayName = "documents";
            this.collections = ko.observableArray();
            this.selectedCollection = ko.observable().subscribeTo("ActivateCollection").distinctUntilChanged();
            this.collectionColors = [];
            this.currentCollectionPagedItems = ko.observable();
            this.subscriptions = [];
            this.ravenDb = new raven();
            this.selectedCollection.subscribe(function (c) {
                return _this.selectedCollectionChanged(c);
            });
        }
        documents.prototype.activate = function (args) {
            var _this = this;
            var dbChangedSubscription = ko.postbox.subscribe("ActivateDatabase", function (db) {
                return _this.databaseChanged(db);
            });
            this.subscriptions.push(dbChangedSubscription);

            // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
            this.collectionToSelectName = args ? args.collection : null;

            if (args && args.database) {
                ko.postbox.publish("ActivateDatabaseWithName", args.database);
            }

            return this.fetchCollections(appUrl.getDatabase());
        };

        documents.prototype.deactivate = function () {
            // Unsubscribe when we leave the page.
            // This is necessary, otherwise our subscriptions will keep the page alive in memory and otherwise screw with us.
            this.subscriptions.forEach(function (s) {
                return s.dispose();
            });
        };

        documents.prototype.attached = function (view, parent) {
            // Initialize the context menu (using Bootstrap-ContextMenu library).
            // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
            ($('.document-collections li')).contextmenu({
                target: '#collections-context-menu'
            });
        };

        documents.prototype.collectionsLoaded = function (collections, db) {
            var _this = this;
            // Set the color class for each of the collections.
            // These styles are found in app.less.
            var collectionStyleCount = 15;
            collections.forEach(function (c, index) {
                return c.colorClass = "collection-style-" + (index % collectionStyleCount);
            });

            // Create the "All Documents" pseudo collection.
            this.allDocumentsCollection = new collection("All Documents");
            this.allDocumentsCollection.isAllDocuments = true;
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

            // Create the "System Documents" pseudo collection.
            var systemDocumentsCollection = new collection("System Documents");
            systemDocumentsCollection.isSystemDocuments = true;

            // All systems a-go. Load them into the UI and select the first one.
            var allCollections = [this.allDocumentsCollection].concat(collections.concat(systemDocumentsCollection));
            this.collections(allCollections);

            var collectionToSelect = collections.first(function (c) {
                return c.name === _this.collectionToSelectName;
            }) || this.allDocumentsCollection;
            collectionToSelect.activate();

            // Fetch the collection info for each collection.
            // The collection info contains information such as total number of documents.
            collections.forEach(function (c) {
                return c.getInfo(db);
            });
        };

        documents.prototype.selectedCollectionChanged = function (selected) {
            if (collection) {
                var fetcher = function (skip, take) {
                    return new getDocumentsCommand(selected, appUrl.getDatabase(), skip, take).execute();
                };
                var documentsList = new pagedList(fetcher);
                documentsList.collectionName = selected.name;
                this.currentCollectionPagedItems(documentsList);
            }
        };

        documents.prototype.databaseChanged = function (db) {
            if (db) {
                // TODO: use appUrl here.
                router.navigate("#documents?database=" + encodeURIComponent(db.name), false);
                this.fetchCollections(db);
            }
        };

        documents.prototype.deleteCollection = function () {
            var _this = this;
            var collection = this.selectedCollection();
            if (collection) {
                var viewModel = new deleteCollection(collection);
                viewModel.deletionTask.done(function () {
                    _this.collections.remove(collection);
                    _this.allDocumentsCollection.activate();
                });
                app.showDialog(viewModel);
            }
        };

        documents.prototype.activateCollection = function (collection) {
            collection.activate();
            var collectionPart = "collection=" + encodeURIComponent(collection.name);
            var databasePart = raven.activeDatabase() ? "&database=" + raven.activeDatabase().name : "";
            router.navigate("#documents?" + collectionPart + databasePart, false);
        };

        documents.prototype.fetchCollections = function (db) {
            var _this = this;
            return this.ravenDb.collections().done(function (results) {
                return _this.collectionsLoaded(results, db);
            });
        };
        return documents;
    })();

    
    return documents;
});
//# sourceMappingURL=documents.js.map
