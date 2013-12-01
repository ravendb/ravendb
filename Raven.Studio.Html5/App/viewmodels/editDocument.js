/// <reference path="../../Scripts/typings/ace/ace.amd.d.ts" />
define(["require", "exports", "durandal/app", "durandal/system", "plugins/router", "ace/ace", "models/document", "models/documentMetadata", "commands/saveDocumentCommand", "common/raven", "viewmodels/deleteDocuments", "common/pagedList", "common/appUrl"], function(require, exports, __app__, __sys__, __router__, __ace__, __document__, __documentMetadata__, __saveDocumentCommand__, __raven__, __deleteDocuments__, __pagedList__, __appUrl__) {
    var app = __app__;
    var sys = __sys__;
    var router = __router__;
    var ace = __ace__;

    var document = __document__;
    var documentMetadata = __documentMetadata__;
    var saveDocumentCommand = __saveDocumentCommand__;
    var raven = __raven__;
    var deleteDocuments = __deleteDocuments__;
    var pagedList = __pagedList__;
    var appUrl = __appUrl__;

    var editDocument = (function () {
        function editDocument() {
            var _this = this;
            this.document = ko.observable();
            this.documentText = ko.observable('');
            this.metadataText = ko.observable('');
            this.isEditingMetadata = ko.observable(false);
            this.isBusy = ko.observable(false);
            this.metaPropsToRestoreOnSave = [];
            this.userSpecifiedId = ko.observable('');
            this.isCreatingNewDocument = ko.observable(false);
            this.docsList = ko.observable();
            this.ravenDb = new raven();
            this.metadata = ko.computed(function () {
                return _this.document() ? _this.document().__metadata : null;
            });

            this.document.subscribe(function (doc) {
                if (doc) {
                    var docText = _this.stringify(doc.toDto());
                    _this.documentText(docText);
                }
            });

            this.metadata.subscribe(function (meta) {
                if (meta) {
                    _this.metaPropsToRestoreOnSave.length = 0;
                    var metaDto = _this.metadata().toDto();

                    // We don't want to show certain reserved properties in the metadata text area.
                    // Remove them from the DTO, restore them on save.
                    var metaPropsToRemove = ["Non-Authoritative-Information", "@id", "Last-Modified", "Raven-Last-Modified", "@etag", "Origin"];
                    metaPropsToRemove.forEach(function (p) {
                        if (metaDto[p]) {
                            delete metaDto[p];
                            _this.metaPropsToRestoreOnSave.push({ name: p, value: metaDto[p] });
                        }
                    });
                    var metaString = _this.stringify(metaDto);
                    _this.metadataText(metaString);
                    _this.userSpecifiedId(meta.id);
                }
            });

            this.editedDocId = ko.computed(function () {
                return _this.metadata() ? _this.metadata().id : '';
            });

            // When we programmatically change the document text or meta text, push it into the editor.
            this.metadataText.subscribe(function () {
                return _this.updateDocEditorText();
            });
            this.documentText.subscribe(function () {
                return _this.updateDocEditorText();
            });
            this.isEditingMetadata.subscribe(function () {
                return _this.updateDocEditorText();
            });
        }
        editDocument.prototype.activate = function (navigationArgs) {
            var _this = this;
            if (navigationArgs && navigationArgs.database) {
                ko.postbox.publish("ActivateDatabaseWithName", navigationArgs.database);
            }

            if (navigationArgs && navigationArgs.list && navigationArgs.item) {
                var itemIndex = parseInt(navigationArgs.item, 10);
                if (!isNaN(itemIndex)) {
                    var collectionName = decodeURIComponent(navigationArgs.list) === "All Documents" ? null : navigationArgs.list;
                    var fetcher = function (skip, take) {
                        return _this.ravenDb.documents(collectionName, skip, take);
                    };
                    var list = new pagedList(fetcher);
                    list.collectionName = navigationArgs.list;
                    list.currentItemIndex(itemIndex);
                    list.getNthItem(0);
                    this.docsList(list);
                }
            }

            if (navigationArgs && navigationArgs.id) {
                return this.loadDocument(navigationArgs.id);
            } else {
                this.editNewDocument();
            }
        };

        // Called when the view is attached to the DOM.
        editDocument.prototype.attached = function () {
            this.initializeDocEditor();
            this.setupKeyboardShortcuts();
        };

        editDocument.prototype.deactivate = function () {
            $("#editDocumentContainer").unbind('keydown.jwerty');
        };

        editDocument.prototype.initializeDocEditor = function () {
            var _this = this;
            // Startup the Ace editor with JSON syntax highlighting.
            this.docEditor = ace.edit("docEditor");
            this.docEditor.setTheme("ace/theme/github");
            this.docEditor.setFontSize("16px");
            this.docEditor.getSession().setMode("ace/mode/json");
            $("#docEditor").on('blur', ".ace_text-input", function () {
                return _this.storeDocEditorTextIntoObservable();
            });
            this.updateDocEditorText();
        };

        editDocument.prototype.setupKeyboardShortcuts = function () {
            var _this = this;
            jwerty.key("ctrl+alt+s", function (e) {
                e.preventDefault();
                _this.saveDocument();
            }, this, "#editDocumentContainer");

            jwerty.key("ctrl+alt+r", function (e) {
                e.preventDefault();
                _this.refreshDocument();
            }, this, "#editDocumentContainer");

            jwerty.key("ctrl+alt+d", function (e) {
                e.preventDefault();
                _this.isEditingMetadata(false);
            });

            jwerty.key("ctrl+alt+m", function (e) {
                e.preventDefault();
                _this.isEditingMetadata(true);
            });
        };

        editDocument.prototype.editNewDocument = function () {
            this.isCreatingNewDocument(true);
            this.document(document.empty());
        };

        editDocument.prototype.failedToLoadDoc = function (docId, errorResponse) {
            sys.log("Failed to load document for editing.", errorResponse);
            app.showMessage("Can't edit '" + docId + "'. Details logged in the browser console.", ":-(", ['Dismiss']);
        };

        editDocument.prototype.saveDocument = function () {
            var _this = this;
            var updatedDto = JSON.parse(this.documentText());
            var meta = JSON.parse(this.metadataText());
            updatedDto['@metadata'] = meta;
            console.log(this.documentText());

            if (this.isCreatingNewDocument()) {
                this.attachReservedMetaProperties(this.userSpecifiedId(), meta);
            } else {
                // If we're editing a document, we hide some reserved properties from the user.
                // Restore these before we save.
                this.metaPropsToRestoreOnSave.forEach(function (p) {
                    return meta[p.name] = p.value;
                });
            }

            var newDoc = new document(updatedDto);
            var saveCommand = new saveDocumentCommand(this.userSpecifiedId(), newDoc);
            var saveTask = saveCommand.execute();
            saveTask.done(function (idAndEtag) {
                _this.isCreatingNewDocument(false);
                _this.loadDocument(idAndEtag.Key);
                _this.updateUrl(idAndEtag.Key);
            });
        };

        editDocument.prototype.attachReservedMetaProperties = function (id, target) {
            target['@etag'] = '00000000-0000-0000-0000-000000000000';
            target['Raven-Entity-Name'] = raven.getEntityNameFromId(id);
            target['@id'] = id;
        };

        editDocument.prototype.stringify = function (obj) {
            var prettifySpacing = 4;
            return JSON.stringify(obj, null, prettifySpacing);
        };

        editDocument.prototype.activateMeta = function () {
            this.isEditingMetadata(true);
        };

        editDocument.prototype.activateDoc = function () {
            this.isEditingMetadata(false);
        };

        editDocument.prototype.loadDocument = function (id) {
            var _this = this;
            var loadDocTask = this.ravenDb.documentWithMetadata(id);
            loadDocTask.done(function (document) {
                return _this.document(document);
            });
            loadDocTask.fail(function (response) {
                return _this.failedToLoadDoc(id, response);
            });
            loadDocTask.always(function () {
                return _this.isBusy(false);
            });
            this.isBusy(true);
            return loadDocTask;
        };

        editDocument.prototype.refreshDocument = function () {
            var meta = this.metadata();
            if (!this.isCreatingNewDocument()) {
                this.document(null);
                this.documentText(null);
                this.metadataText(null);
                this.userSpecifiedId('');
                this.loadDocument(this.editedDocId());
            } else {
                this.editNewDocument();
            }
        };

        editDocument.prototype.deleteDocument = function () {
            var _this = this;
            var doc = this.document();
            if (doc) {
                var viewModel = new deleteDocuments([doc]);
                viewModel.deletionTask.done(function () {
                    _this.nextDocumentOrFirst();
                });
                app.showDialog(viewModel);
            }
        };

        editDocument.prototype.nextDocumentOrFirst = function () {
            var list = this.docsList();
            if (list) {
                var nextIndex = list.currentItemIndex() + 1;
                if (nextIndex >= list.totalResultCount()) {
                    nextIndex = 0;
                }
                this.pageToItem(nextIndex);
            }
        };

        editDocument.prototype.previousDocumentOrLast = function () {
            var list = this.docsList();
            if (list) {
                var previousIndex = list.currentItemIndex() - 1;
                if (previousIndex < 0) {
                    previousIndex = list.totalResultCount() - 1;
                }
                this.pageToItem(previousIndex);
            }
        };

        editDocument.prototype.lastDocument = function () {
            var list = this.docsList();
            if (list) {
                this.pageToItem(list.totalResultCount() - 1);
            }
        };

        editDocument.prototype.firstDocument = function () {
            this.pageToItem(0);
        };

        editDocument.prototype.pageToItem = function (index) {
            var _this = this;
            var list = this.docsList();
            if (list) {
                list.getNthItem(index).done(function (doc) {
                    _this.loadDocument(doc.getId());
                    list.currentItemIndex(index);
                    _this.updateUrl(doc.getId());
                });
            }
        };

        editDocument.prototype.navigateToCollection = function (collectionName) {
            var databaseFragment = raven.activeDatabase() ? "&database=" + raven.activeDatabase().name : "";
            var collectionFragment = collectionName ? "&collection=" + collectionName : "";
            router.navigate("#documents?" + collectionFragment + databaseFragment);
        };

        editDocument.prototype.navigateToDocuments = function () {
            this.navigateToCollection(null);
        };

        editDocument.prototype.updateUrl = function (docId) {
            var collectionName = this.docsList() ? this.docsList().collectionName : null;
            var currentItemIndex = this.docsList() ? this.docsList().currentItemIndex() : null;
            var editDocUrl = appUrl.forEditDoc(docId, collectionName, currentItemIndex);
            router.navigate(editDocUrl, false);
        };

        editDocument.prototype.updateDocEditorText = function () {
            if (this.docEditor) {
                var text = this.isEditingMetadata() ? this.metadataText() : this.documentText();
                this.docEditor.getSession().setValue(text);
            }
        };

        editDocument.prototype.storeDocEditorTextIntoObservable = function () {
            if (this.docEditor) {
                var docEditorText = this.docEditor.getSession().getValue();
                var observableToUpdate = this.isEditingMetadata() ? this.metadataText : this.documentText;
                observableToUpdate(docEditorText);
            }
        };
        return editDocument;
    })();

    
    return editDocument;
});
//# sourceMappingURL=editDocument.js.map
