/// <reference path="../../Scripts/typings/ace/ace.amd.d.ts" />

import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router"); 
import ace = require("ace/ace");

import document = require("models/document");
import database = require("models/database");
import documentMetadata = require("models/documentMetadata");
import collection = require("models/collection");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import deleteDocuments = require("viewmodels/deleteDocuments");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class editDocument extends viewModelBase {

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
    databaseForEditedDoc: database;

    static editDocSelector = "#editDocumentContainer";

    constructor() {
        super();
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
                var metaPropsToRemove = ["@id", "@etag", "Origin", "Raven-Server-Build", "Raven-Client-Version", "Non-Authoritative-Information", "Raven-Timer-Request",
                    "Raven-Authenticated-User", "Raven-Last-Modified", "Has-Api-Key", "Access-Control-Allow-Origin", "Access-Control-Max-Age", "Access-Control-Allow-Methods",
                    "Access-Control-Request-Headers", "Access-Control-Allow-Headers", "Reverse-Via", "Persistent-Auth", "Allow", "Content-Disposition", "Content-Encoding",
                    "Content-Language", "Content-Location", "Content-MD5", "Content-Range", "Content-Type", "Expires", "Last-Modified", "Content-Length", "Keep-Alive", "X-Powered-By",
                    "X-AspNet-Version", "X-Requested-With", "X-SourceFiles", "Accept-Charset", "Accept-Encoding", "Accept", "Accept-Language", "Authorization", "Cookie", "Expect",
                    "From", "Host", "If-MatTemp-Index-Scorech", "If-Modified-Since", "If-None-Match", "If-Range", "If-Unmodified-Since", "Max-Forwards", "Referer", "TE", "User-Agent", "Accept-Ranges",
                    "Age", "Allow", "ETag", "Location", "Retry-After", "Server", "Set-Cookie2", "Set-Cookie", "Vary", "Www-Authenticate", "Cache-Control", "Connection", "Date", "Pragma",
                    "Trailer", "Transfer-Encoding", "Upgrade", "Via", "Warning", "X-ARR-LOG-ID", "X-ARR-SSL", "X-Forwarded-For", "X-Original-URL"];

                for (var property in metaDto) {
                    if (metaDto.hasOwnProperty(property) && metaPropsToRemove.indexOf(property) != -1) {
                        if (metaDto[property]) {
                            this.metaPropsToRestoreOnSave.push({ name: property, value: metaDto[property].toString() });
                        }
                        delete metaDto[property];
                    }
                }
                /*metaPropsToRemove.forEach(p => {
                    if (p in metaDto) {
                        this.metaPropsToRestoreOnSave.push({ name: p, value: metaDto[p].toString() });
                        delete metaDto[p];
                    }
                });*/
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

        super.activate(navigationArgs);

        // Find the database and collection we're supposed to load.
        // Used for paging through items.
        this.databaseForEditedDoc = this.activeDatabase();
        if (navigationArgs && navigationArgs.database) {
            ko.postbox.publish("ActivateDatabaseWithName", navigationArgs.database);
        }

        if (navigationArgs && navigationArgs.list && navigationArgs.item) {
            var itemIndex = parseInt(navigationArgs.item, 10);
            if (!isNaN(itemIndex)) {
                var newCollection = new collection(navigationArgs.list, appUrl.getDatabase());
                var fetcher = (skip: number, take: number) => newCollection.fetchDocuments(skip, take);
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
        this.createKeyboardShortcut("alt+s", () => this.saveDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+r", () => this.refreshDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("shift+d", () => this.isEditingMetadata(false), editDocument.editDocSelector);
        this.createKeyboardShortcut("shift+m", () => this.isEditingMetadata(true), editDocument.editDocSelector);
        this.createKeyboardShortcut("home", () => this.firstDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("end", () => this.lastDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+←", () => this.previousDocumentOrLast(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+→", () => this.nextDocumentOrFirst(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+[", () => this.formatDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("delete", () => this.deleteDocument(), editDocument.editDocSelector);
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
        var saveCommand = new saveDocumentCommand(this.userSpecifiedId(), newDoc, appUrl.getDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            this.isCreatingNewDocument(false);
            this.loadDocument(idAndEtag.Key);
            this.updateUrl(idAndEtag.Key);
        });
    }

    attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = '00000000-0000-0000-0000-000000000000';
        target['Raven-Entity-Name'] = document.getEntityNameFromId(id);
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
        var loadDocTask = new getDocumentWithMetadataCommand(id, this.databaseForEditedDoc).execute();
        loadDocTask.done(document => this.document(document));
        loadDocTask.fail(response => this.failedToLoadDoc(id, response));
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    refreshDocument() {
        var meta = this.metadata();
        if (!this.isCreatingNewDocument()) {
            var docId = this.editedDocId();
            this.document(null);
            this.documentText(null);
            this.metadataText(null);
            this.userSpecifiedId('');
            this.loadDocument(docId);
        } else {
            this.editNewDocument();
        }
    }

    deleteDocument() {
        var doc = this.document();
        if (doc) {
            var viewModel = new deleteDocuments([doc]);
            viewModel.deletionTask.done(() => this.nextDocumentOrFirst());
            app.showDialog(viewModel, editDocument.editDocSelector);
        }
    }

    formatDocument() {
        var docText = this.documentText();
        var tempDoc = JSON.parse(docText);
        var formatted = this.stringify(tempDoc);
        this.documentText(formatted);
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
        var collectionUrl = appUrl.forDocuments(collectionName, this.activeDatabase());
        router.navigate(collectionUrl);
    }

    navigateToDocuments() {
        this.navigateToCollection(null);
    }

    updateUrl(docId: string) {
        var collectionName = this.docsList() ? this.docsList().collectionName : null;
        var currentItemIndex = this.docsList() ? this.docsList().currentItemIndex() : null;
        var editDocUrl = appUrl.forEditDoc(docId, collectionName, currentItemIndex, this.activeDatabase());
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