define(["require", "exports", "durandal/app", "plugins/router", "models/collection", "models/database", "models/document", "viewmodels/deleteCollection", "common/raven", "common/pagedList", "common/appUrl", "commands/getDocumentsCommand", "commands/getCollectionsCommand"], function(require, exports, app, router, collection, database, document, deleteCollection, raven, pagedList, appUrl, getDocumentsCommand, getCollectionsCommand) {
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

            // See if we've got a database to select.
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
            $('.document-collections li').contextmenu({
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
            this.allDocumentsCollection = collection.createAllDocsCollection(db);
            this.allDocumentsCollection.colorClass = "all-documents-collection";
            this.allDocumentsCollection.documentCount = ko.computed(function () {
                return _this.collections().filter(function (c) {
                    return c !== _this.allDocumentsCollection;
                }).map(function (c) {
                    return c.documentCount();
                }).reduce(function (first, second) {
                    return first + second;
                }, 0);
            }); // And sum them up.

            // Create the "System Documents" pseudo collection.
            var systemDocumentsCollection = collection.createSystemDocsCollection(db);
            systemDocumentsCollection.colorClass = "system-documents-collection";

            // All systems a-go. Load them into the UI and select the first one.
            var collectionsWithSysCollection = [systemDocumentsCollection].concat(collections);
            var allCollections = [this.allDocumentsCollection].concat(collectionsWithSysCollection);
            this.collections(allCollections);

            var collectionToSelect = allCollections.first(function (c) {
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
            if (selected) {
                var pagedList = selected.getDocuments();
                this.currentCollectionPagedItems(pagedList);
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
            return new getCollectionsCommand(db).execute().done(function (results) {
                return _this.collectionsLoaded(results, db);
            });
        };
        return documents;
    })();

    
    return documents;
});
//# sourceMappingURL=documents.js.map
