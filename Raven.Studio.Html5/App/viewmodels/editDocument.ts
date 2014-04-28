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
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");
import verifyDocumentsIDsCommand = require("commands/verifyDocumentsIDsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");

class editDocument extends viewModelBase {

    document = ko.observable<document>();
    metadata: KnockoutComputed<documentMetadata>;
    documentText = ko.observable('').extend({ required: true });
    metadataText = ko.observable('').extend({ required: true });
    documentTextSubscription;
    metadataTextSubscription;
    isEditingMetadata = ko.observable(false);
    isBusy = ko.observable(false);
    metaPropsToRestoreOnSave = [];
    editedDocId: KnockoutComputed<string>;
    userSpecifiedId = ko.observable('').extend({ required: true });
    isCreatingNewDocument = ko.observable(false);
    docsList = ko.observable<pagedList>();
    docEditor: AceAjax.Editor;
    databaseForEditedDoc: database;
    topRecentDocuments = ko.computed(() => this.getTopRecentDocuments());
    relatedDocumentHrefs=ko.observableArray<{id:string;href:string}>();
    docEditroHasFocus = ko.observable(true);
    documentMatchRegexp = /\w+\/\w+/ig;
    lodaedDocumentName = ko.observable('');
    isSaveEnabled: KnockoutComputed<Boolean>;

    static editDocSelector = "#editDocumentContainer";
    static recentDocumentsInDatabases = ko.observableArray<{ databaseName: string; recentDocuments: KnockoutObservableArray<string> }>();

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.metadata = ko.computed(() => this.document() ? this.document().__metadata : null);

        this.document.subscribe(doc => {
            if (doc) {
                var docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });

        this.metadata.subscribe((meta: documentMetadata) => this.metadataChanged(meta));
        this.editedDocId = ko.computed(() => this.metadata() ? this.metadata().id : '');
        this.editedDocId.subscribe((docId: string)=> ko.postbox.publish("SetRawJSONUrl", appUrl.forDocumentRawData(this.activeDatabase(), docId)));

        // When we programmatically change the document text or meta text, push it into the editor.
        this.metadataTextSubscription = this.metadataText.subscribe(() => this.updateDocEditorText());
        this.documentTextSubscription = this.documentText.subscribe(() => this.updateDocEditorText());
        this.isEditingMetadata.subscribe(() => this.updateDocEditorText());
    }

    // Called by Durandal when seeing if we can activate this view.
    canActivate(args: any) {
        super.canActivate(args);
        if (args && args.id) {
            var canActivateResult = $.Deferred();
            new getDocumentWithMetadataCommand(args.id, appUrl.getDatabase())
                .execute()
                .done((document) => {
                    this.document(document);
                    canActivateResult.resolve({ can: true });

                    var relatedDocumentsCandidates:string[] = this.findRelatedDocumentsCandidates(document);

                    var docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.activeDatabase(), true, true);
                    var response = docIDsVerifyCommand.execute();

                    if (response.then) {
                        response.done(verifiedIDs => {
                            this.relatedDocumentHrefs(verifiedIDs.map(verified => {
                                return {
                                    id: verified.toString(),
                                    href: appUrl.forEditDoc(verified.toString(), null, null, this.activeDatabase())
                                };
                            }));
                        });
                    } else {

                        this.relatedDocumentHrefs(response.map(verified => {
                            return {
                                id: verified.toString(),
                                href: appUrl.forEditDoc(verified.toString(), null, null, this.activeDatabase())
                            };
                        }));                        
                    }
                    
                })
                .fail(() => {
                    ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not find " + args.id + " document", null));
                    canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocsCollectionName, this.activeDatabase()) });
                }
                );
            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);

        this.lodaedDocumentName(this.userSpecifiedId());
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.documentText, this.metadataText, this.userSpecifiedId]);
        var self = this;
        this.isSaveEnabled = ko.computed(()=> {
            return viewModelBase.dirtyFlag().isDirty() && !!self.userSpecifiedId();
        });

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
            var existingRecentDocumentsStore = editDocument.recentDocumentsInDatabases.first(x=> x.databaseName == this.databaseForEditedDoc.name);
            if (existingRecentDocumentsStore) {
                var existingDocumentInStore = existingRecentDocumentsStore.recentDocuments.first(x=> x === navigationArgs.id);
                if (!existingDocumentInStore) {
                    if (existingRecentDocumentsStore.recentDocuments().length == 5) {
                        existingRecentDocumentsStore.recentDocuments.pop();
                    }
                    existingRecentDocumentsStore.recentDocuments.unshift(navigationArgs.id);
                }

            } else {
                editDocument.recentDocumentsInDatabases.push({ databaseName: this.databaseForEditedDoc.name, recentDocuments: ko.observableArray([navigationArgs.id]) });
            }

            ko.postbox.publish("SetRawJSONUrl", appUrl.forDocumentRawData(this.activeDatabase(), navigationArgs.id));
            return true;
        } else {
            this.editNewDocument();
        }
    }

    // Called when the view is attached to the DOM.
    attached() {
        this.initializeDocEditor();
        this.setupKeyboardShortcuts();
        this.focusOnEditor();
    }

    initializeDocEditor() {
        // Startup the Ace editor with JSON syntax highlighting.
        this.docEditor = ace.edit("docEditor");
        this.docEditor.setTheme("ace/theme/github");
        this.docEditor.setFontSize("16px");
        this.docEditor.getSession().setMode("ace/mode/json");
        $("#docEditor").on('keyup', ".ace_text-input", () => this.storeDocEditorTextIntoObservable());
        this.updateDocEditorText();
    }

    setupKeyboardShortcuts() {        
        this.createKeyboardShortcut("alt+shift+d", () => this.focusOnDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+m", () => this.focusOnMetadata(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+home", () => this.firstDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+end", () => this.lastDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+page-up", () => this.previousDocumentOrLast(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+page-down", () => this.nextDocumentOrFirst(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteDocument(), editDocument.editDocSelector);
    }

    focusOnMetadata() {
        this.isEditingMetadata(true);
        this.focusOnEditor();
    }

    focusOnDocument() {
        this.isEditingMetadata(false);
        this.focusOnEditor();
    }

    focusOnEditor() {
        this.docEditor.focus();
    }

    editNewDocument() {
        this.isCreatingNewDocument(true);
        this.document(document.empty());
    }

    failedToLoadDoc(docId, errorResponse) {
        ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not find " + docId + " document", null));
    }

    saveDocument() {
        //the name of the document was changed and we have to save it as a new one
        var meta = JSON.parse(this.metadataText());
        var currentDocumentId = this.userSpecifiedId();
        if (!!this.lodaedDocumentName() && this.lodaedDocumentName() != currentDocumentId) {
            this.isCreatingNewDocument(true);
        }

        var updatedDto = JSON.parse(this.documentText());
        updatedDto['@metadata'] = meta;

        // Fix up the metadata: if we're a new doc, attach the expected reserved properties like ID, ETag, and RavenEntityName.
        // AFAICT, Raven requires these reserved meta properties in order for the doc to be seen as a member of a collection.
        if (this.isCreatingNewDocument()) {
            this.attachReservedMetaProperties(currentDocumentId, meta);
        } else {
            // If we're editing a document, we hide some reserved properties from the user.
            // Restore these before we save.
            this.metaPropsToRestoreOnSave.forEach(p => {
                if (p.name !== "Origin"){
                    meta[p.name] = p.value;
                }
            });
        }

        // skip some not necessary meta in headers
        var metaToSkipInHeaders = ['Raven-Replication-History'];
        for (var i in metaToSkipInHeaders) {
            var skippedHeader = metaToSkipInHeaders[i];
            delete meta[skippedHeader];
        }

        var newDoc = new document(updatedDto);
        var saveCommand = new saveDocumentCommand(currentDocumentId, newDoc, appUrl.getDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            // Resync Changes
            viewModelBase.dirtyFlag().reset();

            this.lodaedDocumentName(currentDocumentId);
            this.isCreatingNewDocument(false);
            this.loadDocument(idAndEtag.Key);
            this.updateUrl(idAndEtag.Key);
        });
    }

    attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = '';
        target['Raven-Entity-Name'] = !target['Raven-Entity-Name'] ? document.getEntityNameFromId(id) : target['Raven-Entity-Name'];
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

    findRelatedDocumentsCandidates(doc: documentBase): string[] {
        var results: string[] = [];
        var initialDocumentFields = doc.getDocumentPropertyNames();
        var documentNodesFlattenedList = [];

        // get initial nodes list to work with
        initialDocumentFields.forEach(curField => {
            documentNodesFlattenedList.push(doc[curField]);
        });

        
        for (var documentNodesCursor = 0; documentNodesCursor < documentNodesFlattenedList.length; documentNodesCursor++) {
            var curField = documentNodesFlattenedList[documentNodesCursor];
            if (typeof curField === "string" && /\w+\/\w+/ig.test(curField)) {
                
                if (!results.first(x=>x === curField.toString())){
                    results.push(curField.toString());
                }
            }
            else if (typeof curField == "object" && !!curField) {
                    for (var curInnerField in curField) {
                        documentNodesFlattenedList.push(curField[curInnerField]);
                    }
            }
        }
        return results;
    }

    loadDocument(id: string): JQueryPromise<document> {
        var loadDocTask = new getDocumentWithMetadataCommand(id, this.databaseForEditedDoc).execute();
        loadDocTask.done(document=> {
            this.document(document);

            // Resync Changes
            viewModelBase.dirtyFlag().reset();
        });
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

        // Resync Changes
        viewModelBase.dirtyFlag().reset();
    }

    formatDocument() {
        var docEditorText = this.docEditor.getSession().getValue();
        var observableToUpdate = this.isEditingMetadata() ? this.metadataText : this.documentText;
        var tempDoc = JSON.parse(docEditorText);
        var formatted = this.stringify(tempDoc);
        observableToUpdate(formatted);
    }

    nextDocumentOrFirst() {
        var list = this.docsList(); 
        if (list) {
            var nextIndex = list.currentItemIndex() + 1;
            if (nextIndex >= list.totalResultCount()) {
                nextIndex = 0;
            }
            this.pageToItem(nextIndex);
        } else {
            this.navigateToDocuments();
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
            var subscription = this.isEditingMetadata() ? this.metadataTextSubscription : this.documentTextSubscription;

            subscription.dispose();
            observableToUpdate(docEditorText);
            if (this.isEditingMetadata()) {
                this.metadataTextSubscription = this.metadataText.subscribe(() => this.updateDocEditorText());
            } else {
                this.documentTextSubscription = this.documentText.subscribe(() => this.updateDocEditorText());
            }
        }
    }

    getTopRecentDocuments() {
        var currentDbName = this.activeDatabase().name;
        var recentDocumentsForCurDb = editDocument.recentDocumentsInDatabases().first(x => x.databaseName === currentDbName);
        if (recentDocumentsForCurDb) {
            var value = recentDocumentsForCurDb
                .recentDocuments()
                .filter((x:string) => {
                  return x !== this.userSpecifiedId();
                })
                .slice(0, 5)
                .map((docId: string) => {
                    return {
                        docId: docId,
                        docUrl: appUrl.forEditDoc(docId, null, null, this.activeDatabase())
                    };
                });
            return value;
        } else {
            return [];
        }
    }

    metadataChanged(meta: documentMetadata) {
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
                if (metaDto.hasOwnProperty(property) && metaPropsToRemove.contains(property)) {
                    if (metaDto[property]) {
                        this.metaPropsToRestoreOnSave.push({ name: property, value: metaDto[property].toString() });
                    }
                    delete metaDto[property];
                }
            }

            var metaString = this.stringify(metaDto);
            this.metadataText(metaString);
            this.userSpecifiedId(meta.id);
        }
    }
}

export = editDocument;