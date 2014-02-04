var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "durandal/app", "plugins/router", "models/collection", "models/database", "models/document", "viewmodels/deleteCollection", "common/pagedList", "common/appUrl", "commands/getCollectionsCommand", "viewmodels/viewModelBase", "widgets/virtualTable/viewModel"], function(require, exports, app, router, collection, database, document, deleteCollection, pagedList, appUrl, getCollectionsCommand, viewModelBase, virtualTable) {
    var documents = (function (_super) {
        __extends(documents, _super);
        function documents() {
            var _this = this;
            _super.call(this);
            this.displayName = "documents";
            this.collections = ko.observableArray();
            this.selectedCollection = ko.observable().subscribeTo("ActivateCollection").distinctUntilChanged();
            this.currentCollectionPagedItems = ko.observable();
            this.selectedDocumentIndices = ko.observableArray();
            this.isSelectAll = ko.observable(false);
            this.selectedCollection.subscribe(function (c) {
                return _this.selectedCollectionChanged(c);
            });
            this.hasAnyDocumentsSelected = ko.computed(function () {
                return _this.selectedDocumentIndices().length > 0;
            });
        }
        documents.prototype.activate = function (args) {
            var _this = this;
            _super.prototype.activate.call(this, args);
            this.activeDatabase.subscribe(function (db) {
                return _this.databaseChanged(db);
            });

            // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
            this.collectionToSelectName = args ? args.collection : null;

            return this.fetchCollections(appUrl.getDatabase());
        };

        documents.prototype.attached = function (view, parent) {
            // Initialize the context menu (using Bootstrap-ContextMenu library).
            // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
            $('.document-collections li').contextmenu({
                target: '#collections-context-menu'
            });

            this.useBootstrapTooltips();
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
            collectionsWithSysCollection.forEach(function (c) {
                return c.fetchTotalDocumentCount();
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
                var documentsUrl = appUrl.forDocuments(null, db);
                router.navigate(documentsUrl, false);
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

        documents.prototype.selectCollection = function (collection) {
            collection.activate();
            var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
            router.navigate(documentsWithCollectionUrl, false);
        };

        documents.prototype.fetchCollections = function (db) {
            var _this = this;
            return new getCollectionsCommand(db).execute().done(function (results) {
                return _this.collectionsLoaded(results, db);
            });
        };

        documents.prototype.newDocument = function () {
            router.navigate(appUrl.forNewDoc(this.activeDatabase()));
        };

        documents.prototype.toggleSelectAll = function () {
            this.isSelectAll.toggle();

            var docsGrid = this.getDocumentsGrid();
            if (docsGrid && this.isSelectAll()) {
                docsGrid.selectAll();
            } else if (docsGrid && !this.isSelectAll()) {
                docsGrid.selectNone();
            }
        };

        documents.prototype.editSelectedDoc = function () {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.editLastSelectedDoc();
            }
        };

        documents.prototype.deleteSelectedDocs = function () {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.deleteSelectedDocs();
            }
        };

        documents.prototype.copySelectedDocs = function () {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.copySelectedDocs();
            }
        };

        documents.prototype.copySelectedDocIds = function () {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.copySelectedDocIds();
            }
        };

        documents.prototype.getDocumentsGrid = function () {
            var gridContents = $(documents.gridSelector).children()[0];
            if (gridContents) {
                return ko.dataFor(gridContents);
            }

            return null;
        };
        documents.gridSelector = "#documentsGrid";
        return documents;
    })(viewModelBase);

    
    return documents;
});
//# sourceMappingURL=documents.js.map
