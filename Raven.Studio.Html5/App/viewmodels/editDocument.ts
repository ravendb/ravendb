/// <reference path="../../Scripts/typings/ace/ace.amd.d.ts" />

import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router"); 
import ace = require("ace/ace");

import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import raven = require("common/raven");
import deleteDocuments = require("viewmodels/deleteDocuments");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");

class editDocument {

    ravenDb: raven;
    document = ko.observable<document>();
    metadata: KnockoutComputed<documentMetadata>;
    documentText = ko.observable('');
    metadataText = ko.observable('');
    isEditingMetadata = ko.observable(false);
    isBusy = ko.observable(false);
    metaPropsToRestoreOnSave = [];
    editedDocId: KnockoutComputed<string>;
    userSpecifiedId = ko.observable('');
    isCreatingNewDocument = ko.observable(false);
    docsList = ko.observable<pagedList>();
    docEditor: AceAjax.Editor;

    constructor() {
        this.ravenDb = new raven();
        this.metadata = ko.computed(() => this.document() ? this.document().__metadata : null);

        this.document.subscribe(doc => {
            if (doc) {
                var docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });

        this.metadata.subscribe((meta: documentMetadata) => {
            if (meta) {
                this.metaPropsToRestoreOnSave.length = 0;
                var metaDto = this.metadata().toDto();

                // We don't want to show certain reserved properties in the metadata text area.
                // Remove them from the DTO, restore them on save.
                var metaPropsToRemove = ["Non-Authoritative-Information", "@id", "Last-Modified", "Raven-Last-Modified", "@etag", "Origin"];
                metaPropsToRemove.forEach(p => {
                    if (metaDto[p]) {
                        delete metaDto[p];
                        this.metaPropsToRestoreOnSave.push({ name: p, value: metaDto[p] });
                    }
                });
                var metaString = this.stringify(metaDto);
                this.metadataText(metaString);
                this.userSpecifiedId(meta.id);
            }
        });

        this.editedDocId = ko.computed(() => this.metadata() ? this.metadata().id : '');

        // When we programmatically change the document text or meta text, push it into the editor.
        this.metadataText.subscribe(() => this.updateDocEditorText());
        this.documentText.subscribe(() => this.updateDocEditorText());
        this.isEditingMetadata.subscribe(() => this.updateDocEditorText());
    }

    activate(navigationArgs) {

        // Find the database and collection we're supposed to load.
        // Used for paging through items.
        if (navigationArgs && navigationArgs.database) {
            ko.postbox.publish("ActivateDatabaseWithName", navigationArgs.database);
        }

        if (navigationArgs && navigationArgs.list && navigationArgs.item) {
            var itemIndex = parseInt(navigationArgs.item, 10);
            if (!isNaN(itemIndex)) {
                var collectionName = decodeURIComponent(navigationArgs.list) === "All Documents" ? null : navigationArgs.list;
                var fetcher = (skip: number, take: number) => this.ravenDb.documents(collectionName, skip, take);
                var list = new pagedList(fetcher);
                list.collectionName = navigationArgs.list;
                list.currentItemIndex(itemIndex);
                list.getNthItem(0); // Force us to get the total items count.
                this.docsList(list);
            }
        }
		
        if (navigationArgs && navigationArgs.id) {
            return this.loadDocument(navigationArgs.id);
        } else {
            this.editNewDocument();
        }
    }

    // Called when the view is attached to the DOM.
    attached() {
        this.initializeDocEditor();
        this.setupKeyboardShortcuts();
    }

    deactivate() {
        $("#editDocumentContainer").unbind('keydown.jwerty');
    }

    initializeDocEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        this.docEditor = ace.edit("docEditor");
        this.docEditor.setTheme("ace/theme/github");
        this.docEditor.setFontSize("16px");
        this.docEditor.getSession().setMode("ace/mode/json");
        $("#docEditor").on('blur', ".ace_text-input", () => this.storeDocEditorTextIntoObservable());
        this.updateDocEditorText();
    }

    setupKeyboardShortcuts() {
        jwerty.key("ctrl+alt+s", e => {
            e.preventDefault();
            this.saveDocument();
        }, this, "#editDocumentContainer");

        jwerty.key("ctrl+alt+r", e => {
            e.preventDefault();
            this.refreshDocument();
        }, this, "#editDocumentContainer");

        jwerty.key("ctrl+alt+d", e => {
            e.preventDefault();
            this.isEditingMetadata(false);
        });

        jwerty.key("ctrl+alt+m", e => {
            e.preventDefault();
            this.isEditingMetadata(true);
        });
    }

    editNewDocument() {
        this.isCreatingNewDocument(true);
        this.document(document.empty());
    }

    failedToLoadDoc(docId, errorResponse) {
        sys.log("Failed to load document for editing.", errorResponse);
        app.showMessage("Can't edit '" + docId + "'. Details logged in the browser console.", ":-(", ['Dismiss']);
    }

    saveDocument() {
        var updatedDto = JSON.parse(this.documentText());
        var meta = JSON.parse(this.metadataText());
        updatedDto['@metadata'] = meta;
        console.log(this.documentText());

        // Fix up the metadata: if we're a new doc, attach the expected reserved properties like ID, ETag, and RavenEntityName.
        // AFAICT, Raven requires these reserved meta properties in order for the doc to be seen as a member of a collection.
        if (this.isCreatingNewDocument()) {
            this.attachReservedMetaProperties(this.userSpecifiedId(), meta);
        } else {
            // If we're editing a document, we hide some reserved properties from the user.
            // Restore these before we save.
            this.metaPropsToRestoreOnSave.forEach(p => meta[p.name] = p.value);
        }

        var newDoc = new document(updatedDto);
        var saveCommand = new saveDocumentCommand(this.userSpecifiedId(), newDoc);
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            this.isCreatingNewDocument(false);
            this.loadDocument(idAndEtag.Key);
            this.updateUrl(idAndEtag.Key);
        });
    }

    attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = '00000000-0000-0000-0000-000000000000';
        target['Raven-Entity-Name'] = raven.getEntityNameFromId(id);
        target['@id'] = id;
    }

    stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    activateMeta() {
        this.isEditingMetadata(true);
    }

    activateDoc() {
        this.isEditingMetadata(false);
    }

    loadDocument(id: string): JQueryPromise<document> {
        var loadDocTask = this.ravenDb.documentWithMetadata(id);
        loadDocTask.done(document => this.document(document));
        loadDocTask.fail(response => this.failedToLoadDoc(id, response));
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    refreshDocument() {
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
    }

    deleteDocument() {
        var doc = this.document();
        if (doc) {
            var viewModel = new deleteDocuments([doc]);
            viewModel.deletionTask.done(() => {
                this.nextDocumentOrFirst();

            });
            app.showDialog(viewModel);
        }
    }

    nextDocumentOrFirst() {
        var list = this.docsList(); 
        if (list) {
            var nextIndex = list.currentItemIndex() + 1;
            if (nextIndex >= list.totalResultCount()) {
                nextIndex = 0;
            }
            this.pageToItem(nextIndex);
        }
    }

    previousDocumentOrLast() {
        var list = this.docsList();
        if (list) {
            var previousIndex = list.currentItemIndex() - 1;
            if (previousIndex < 0) {
                previousIndex = list.totalResultCount() - 1;
            }
            this.pageToItem(previousIndex);
        }
    }

    lastDocument() {
        var list = this.docsList();
        if (list) {
            this.pageToItem(list.totalResultCount() - 1);
        }
    }

    firstDocument() {
        this.pageToItem(0);
    }

    pageToItem(index: number) {
        var list = this.docsList();
        if (list) {
            list
                .getNthItem(index)
                .done((doc: document) => {
                    this.loadDocument(doc.getId());
                    list.currentItemIndex(index);
                    this.updateUrl(doc.getId());
                });
        }
    }

    navigateToCollection(collectionName: string) {
        var databaseFragment = raven.activeDatabase() ? "&database=" + raven.activeDatabase().name : "";
        var collectionFragment = collectionName ? "&collection=" + collectionName : "";
        router.navigate("#documents?" + collectionFragment + databaseFragment);
    }

    navigateToDocuments() {
        this.navigateToCollection(null);
    }

    updateUrl(docId: string) {
        var collectionName = this.docsList() ? this.docsList().collectionName : null;
        var currentItemIndex = this.docsList() ? this.docsList().currentItemIndex() : null;
        var editDocUrl = appUrl.forEditDoc(docId, collectionName, currentItemIndex);
        router.navigate(editDocUrl, false);
    }

    updateDocEditorText() {
        if (this.docEditor) {
            var text = this.isEditingMetadata() ? this.metadataText() : this.documentText();
            this.docEditor.getSession().setValue(text);
        }
    }

    storeDocEditorTextIntoObservable() {
        if (this.docEditor) {
            var docEditorText = this.docEditor.getSession().getValue();
            var observableToUpdate = this.isEditingMetadata() ? this.metadataText : this.documentText;
            observableToUpdate(docEditorText);
        }
    }
}

export = editDocument;