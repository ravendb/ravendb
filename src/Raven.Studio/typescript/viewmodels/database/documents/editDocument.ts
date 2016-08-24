import app = require("durandal/app");
import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import documentMetadata = require("models/database/documents/documentMetadata");
import collection = require("models/database/documents/collection");
import querySort = require("models/database/query/querySort");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import resolveMergeCommand = require("commands/database/studio/resolveMergeCommand");
import generateClassCommand = require("commands/database/documents/generateClassCommand");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import genUtils = require("common/generalUtils");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import documentHelpers = require("common/helpers/database/documentHelpers");

import deleteDocuments = require("viewmodels/common/deleteDocuments");
import viewModelBase = require("viewmodels/viewModelBase");
import showDataDialog = require("viewmodels/common/showDataDialog");

class editDocument extends viewModelBase {

    static editDocSelector = "#editDocumentContainer";
    static documentNameSelector = "#documentName";
    static docEditorSelector = "#docEditor";

    document = ko.observable<document>();
    documentText = ko.observable("");
    metadata = ko.pureComputed<documentMetadata>(() => this.document() ? this.document().__metadata : null);

    isCreatingNewDocument = ko.observable(false);
    userSpecifiedId = ko.observable("");
    docEditor: AceAjax.Editor;

    $documentName: JQuery;
    $docEditor: JQuery;

    isConflictDocument = ko.observable<boolean>();
    
    metaPropsToRestoreOnSave: any[] = [];
    editedDocId: KnockoutComputed<string>;
    loadedDocumentName = ko.observable<string>("");

    isInDocMode = ko.observable(true);
    databaseForEditedDoc: database;

    docMode = {
        docsList: ko.observable<pagedList>()
    };

    queryMode = {
        queryIndex: ko.observable<string>(),
        queryResultList: ko.observable<pagedList>(),
        currentQueriedItemIndex: undefined as number    
    };
    
    recentDocuments = new recentDocumentsCtr();
    topRecentDocuments = ko.pureComputed(() => this.recentDocuments.getTopRecentDocuments(this.activeDatabase(), this.userSpecifiedId()));
    relatedDocumentHrefs = ko.observableArray<{ id: string; href: string }>();
    
    isSaveEnabled: KnockoutComputed<boolean>;
    documentSize: KnockoutComputed<string>;
    docTitle: KnockoutComputed<string>;
    isNewLineFriendlyMode = ko.observable(false);
    autoCollapseMode = ko.observable(false);
    newLineToggle = '\\n'; //TODO: remove me?
    isSystemDocumentByDocTitle = ko.observable(false); //TODO: do we need it?
    isSaving = ko.observable<boolean>(false);
    documentChangeNotificationMessage = ko.observable<string>();
    changeNotification: changeSubscription;
    
    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.initializeObservables();
    }

    canActivate(args: any) {
        super.canActivate(args);

        if (args && args.id) {
            return this.activateById(args.id);
        } else if (args && args.item && args.list) {
            return $.Deferred().resolve({ can: true });
        } else if (args && args.index) {
            return this.activateByIndex(args.index, args.query, args.sorts, args.item);
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(navigationArgs: any) {
        super.activate(navigationArgs);
        this.updateHelpLink('M72H1R');

        this.loadedDocumentName(this.userSpecifiedId());
        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.userSpecifiedId], false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => this.dirtyFlag().isDirty() || !!this.loadedDocumentName());

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
                this.docMode.docsList(list);
            }
        }

        if (navigationArgs && navigationArgs.id) {
            this.recentDocuments.appendRecentDocument(this.databaseForEditedDoc, navigationArgs.id);

            ko.postbox.publish("SetRawJSONUrl", appUrl.forDocumentRawData(this.activeDatabase(), navigationArgs.id));
        } else {
            this.editNewDocument();
        }
    }

    // Called when the view is attached to the DOM.
    attached() {
        super.attached();
        this.setupKeyboardShortcuts();

        this.$documentName = $(editDocument.documentNameSelector);
        this.$docEditor = $(editDocument.docEditorSelector);

        this.$docEditor.resize(); //TODO: avoid this event
        this.isNewLineFriendlyMode.subscribe(val => {
            this.updateNewlineLayoutInDocument(val);
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.docEditor = aceEditorBindingHandler.getEditorBySelection(this.$docEditor);

        this.$docEditor.on('DynamicHeightSet', () => this.docEditor.resize());
        this.$docEditor.bind("paste", function () { this.text.valueHasMutated() }); //TODO: test me!
        this.focusOnEditor();
    }

    detached() {
        super.detached();
        this.$docEditor.off('DynamicHeightSet');
    }

    private activateById(id: string) {
        var canActivateResult = $.Deferred();
        this.databaseForEditedDoc = appUrl.getDatabase();
        this.loadDocument(id)
            .done(() => {
                //TODO:this.changeNotification = this.createDocumentChangeNotification(args.id);
                this.addNotification(this.changeNotification);
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                messagePublisher.reportError("Could not find " + id + " document");
                canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocsCollectionName, this.activeDatabase()) });
            });
        return canActivateResult;
    }

    private activateByIndex(indexName: string, query: string, argSorts: string, argItem: any) {
        var canActivateResult = $.Deferred();
        this.isInDocMode(false);
        var sorts: querySort[];

        if (argSorts) {
            sorts = argSorts.split(",").map((curSort: string) => querySort.fromQuerySortString(curSort.trim()));
        } else {
            sorts = [];
        }

        var resultsFetcher = (skip: number, take: number) => {
            var command = new queryIndexCommand(indexName, this.activeDatabase(), skip, take, query, sorts);
            return command
                .execute();
        };
        var list = new pagedList(resultsFetcher);
        var item = !!argItem && !isNaN(argItem) ? argItem : 0;

        list.getNthItem(item)
            .done((doc: document) => {
                this.document(doc);
                this.loadedDocumentName("");
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                messagePublisher.reportError("Could not find query result");
                canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocsCollectionName, this.activeDatabase()) });
            });
        this.queryMode.currentQueriedItemIndex = item;
        this.queryMode.queryResultList(list);
        this.queryMode.queryIndex(indexName);
        return canActivateResult;
    }

    private initializeObservables(): void {
        this.isConflictDocument = ko.computed(() => {
            var metadata = this.metadata();
            return metadata && ("Raven-Replication-Conflict" in metadata) && !metadata.id.contains("/conflicts/");
        });

        this.document.subscribe(doc => {
            if (doc) {
                if (this.isConflictDocument()) {
                    this.resolveConflicts();
                } else {
                    var docText = this.stringify(doc.toDto(true));
                    this.documentText(docText);
                }
            }
        });

        this.documentSize = ko.pureComputed(() => {
            try {
                var size: number = this.documentText().getSizeInBytesAsUTF8() / 1024;
                return genUtils.formatAsCommaSeperatedString(size, 2);
            } catch (e) {
                return "cannot compute";
            }
        });

        this.metadata.subscribe((meta: documentMetadata) => this.metadataChanged(meta));
        this.editedDocId = ko.pureComputed(() => this.metadata() ? this.metadata().id : "");
        this.editedDocId.subscribe((docId: string) =>
            ko.postbox.publish("SetRawJSONUrl", docId ? appUrl.forDocumentRawData(this.activeDatabase(), docId) : "")
        );

        this.docTitle = ko.pureComputed<string>(() => {
            if (this.isInDocMode()) {
                if (this.isCreatingNewDocument()) {
                    this.isSystemDocumentByDocTitle(false);
                    return "New Document";
                } else {
                    var editedDocId = this.editedDocId();

                    if (editedDocId) {
                        this.isSystemDocumentByDocTitle(editedDocId.startsWith("Raven/"));

                        var lastIndexInEditedDocId = editedDocId.lastIndexOf("/") + 1;
                        if (lastIndexInEditedDocId > 0) {
                            editedDocId = editedDocId.slice(lastIndexInEditedDocId);
                        }
                    }

                    return editedDocId;
                }
            } else {
                return "Projection";
            }
        });
    }


    createDocumentChangeNotification(docId: string) {
        return changesContext.currentResourceChangesApi().watchDocument(docId, (n: documentChangeNotificationDto) => this.documentChangeNotification(n));
    }

    documentChangeNotification(n: documentChangeNotificationDto): void {
        if (this.isSaving()) {
            return;
        }

        var newEtag = n.Etag;
        if (newEtag === this.metadata().etag) {
            return;
        }

        var message = "Document was changed, new Etag: " + n.Etag;
        this.documentChangeNotificationMessage(message);
        $(".changeNotification").highlight();
    }

    updateNewlineLayoutInDocument(unescapeNewline: boolean) {
        var dirtyFlagValue = this.dirtyFlag().isDirty();
        if (unescapeNewline) {
            this.documentText(documentHelpers.unescapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/json_newline_friendly');
        } else {
            this.documentText(documentHelpers.escapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/json');
            this.formatDocument();
        }

        if (!dirtyFlagValue) {
            this.dirtyFlag().reset();
        }
    }

    setupKeyboardShortcuts() {       
        this.createKeyboardShortcut("alt+shift+r", () => this.refreshDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+s", () => this.saveDocument(), editDocument.editDocSelector); // Q. Why do we have to setup ALT+S, when we could just use HTML's accesskey attribute? A. Because the accesskey attribute causes the save button to take focus, thus stealing the focus from the user's editing spot in the doc editor, disrupting his workflow.
    }

    private focusOnEditor() {
        this.docEditor.focus();
    }

    editNewDocument() {
        this.isCreatingNewDocument(true);

        //TODO: create document stucture by sampling previous values 

        var newDocument: any = document.empty();
        newDocument["Name"] = "...";
        //TODO: insert metadata ?
        this.document(newDocument);
    }

    toggleNewlineMode() {
        if (this.isNewLineFriendlyMode() === false && parseInt(this.documentSize().replace(",", "")) > 150) {
            app.showMessage("This operation might take long time with big documents, are you sure you want to continue?", "Toggle newline mode", ["Cancel", "Continue"])
                .then((dialogResult: string) => {
                    if (dialogResult === "Continue") {
                        this.isNewLineFriendlyMode.toggle();
                    }
                });
        } else {
            this.isNewLineFriendlyMode.toggle();
        }
    }

    toggleAutoCollapse() {
        this.autoCollapseMode.toggle();
        if (this.autoCollapseMode()) {
            this.foldAll();
        } else {
            this.docEditor.getSession().unfold(null, true);
        }
    }

    foldAll() {
        var AceRange = require("ace/range").Range;
        this.docEditor.getSession().foldAll();
        var folds = <any[]> this.docEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.docEditor.getSession().getLength(), 0));
        folds.map(f => this.docEditor.getSession().expandFold(f));
    }

    saveDocument() {
        this.isInDocMode(true);
        var currentDocumentId = this.userSpecifiedId();
        var loadedDocumentName = this.loadedDocumentName();
        if (!currentDocumentId || loadedDocumentName !== currentDocumentId) {
            //the name of the document was changed and we have to save it as a new one
            this.isCreatingNewDocument(true);
        }

        let message = "";
        var updatedDto: any;

        if (currentDocumentId.contains("\\")) { //TODO: use validation hints for this!
            message = "Document name cannot contain '\\'";
            this.$documentName.focus();
        } else {
            try {
                if (this.isNewLineFriendlyMode()) {
                    updatedDto = JSON.parse(documentHelpers.escapeNewlinesAndTabsInTextFields(this.documentText()));
                } else {
                    updatedDto = JSON.parse(this.documentText());
                }
            } catch (e) {
                if (updatedDto == undefined) {
                    message = "The document data isn't a legal JSON expression!";
                }
                this.focusOnEditor();
            }
        }
        
        if (message) {
            messagePublisher.reportError(message, undefined, undefined, false);
            return;
        }

        let meta = updatedDto['@metadata'];

        // Fix up the metadata: if we're a new doc, attach the expected reserved properties like ID, ETag, and RavenEntityName.
        // AFAICT, Raven requires these reserved meta properties in order for the doc to be seen as a member of a collection.
        if (this.isCreatingNewDocument()) {
            this.attachReservedMetaProperties(currentDocumentId, meta);
        } else {
            // If we're editing a document, we hide some reserved properties from the user.
            // Restore these before we save.
            this.metaPropsToRestoreOnSave.forEach(p => {
                if (p.name !== "Origin") {
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

        if (this.docMode.docsList()) {
            this.docMode.docsList().invalidateCache();
        }

        var newDoc = new document(updatedDto);
        var saveCommand = new saveDocumentCommand(currentDocumentId, newDoc, this.activeDatabase());
        this.isSaving(true);
        var saveTask = saveCommand.execute();
        saveTask.done((saveResult: bulkDocumentDto[]) => {
            var isCreatingNewDocument = this.isCreatingNewDocument();
            var savedDocumentDto: bulkDocumentDto = saveResult[0];
            var currentSelection = this.docEditor.getSelectionRange();
            this.loadDocument(savedDocumentDto.Key)
                .done(() => {
                    if (isCreatingNewDocument === false) {
                        return;
                    }

                    if (!!loadedDocumentName) {
                        this.changeNotification.off();
                        this.removeNotification(this.changeNotification);
                    }

                    //TODO: this.changeNotification = this.createDocumentChangeNotification(savedDocumentDto.Key);
                    this.addNotification(this.changeNotification);
                })
                .always(() => {
                    this.updateNewlineLayoutInDocument(this.isNewLineFriendlyMode());

                    // Try to restore the selection.
                    this.docEditor.selection.setRange(currentSelection, false);
                    this.isSaving(false);
                });
            this.updateUrl(savedDocumentDto.Key);

            this.dirtyFlag().reset(); //Resync Changes

            // add the new document to the paged list
            var list: pagedList = this.docMode.docsList();
            if (!!list) {
                if (this.isCreatingNewDocument()) {
                    var newTotalResultCount = list.totalResultCount() + 1;

                    list.totalResultCount(newTotalResultCount);
                    list.currentItemIndex(newTotalResultCount - 1);
                    
                } else {
                    list.currentItemIndex(list.totalResultCount() - 1);
                }

                this.updateUrl(currentDocumentId);
            }

            this.isCreatingNewDocument(false);
        });
    }

    private attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = '00000000-0000-0000-0000-000000000000';
        target['Raven-Entity-Name'] = target['Raven-Entity-Name'] || document.getEntityNameFromId(id);
        target['@id'] = id;
    }

    stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    loadDocument(id: string): JQueryPromise<document> {
        this.isBusy(true);

        return new getDocumentWithMetadataCommand(id, this.databaseForEditedDoc)
            .execute()
            .done((doc: document) => {
                this.document(doc);
                this.loadedDocumentName(this.userSpecifiedId());
                this.dirtyFlag().reset();

                this.loadRelatedDocumentsList(doc);
                this.recentDocuments.appendRecentDocument(this.databaseForEditedDoc, id);
                if (this.autoCollapseMode()) {
                    this.foldAll();
                }
            })
            .fail(() => messagePublisher.reportError("Could not find " + id + " document"))
            .always(() => this.isBusy(false));
    }

    refreshDocument() {
        var canContinue = this.canContinueIfNotDirty("Refresh", "You have unsaved data. Are you sure you want to continue?");
        canContinue.done(() => {
            if (this.isInDocMode()) {
                if (!this.isCreatingNewDocument()) {
                    var docId = this.editedDocId();
                    this.document(null);
                    this.documentText(null);
                    this.userSpecifiedId("");
                    this.loadDocument(docId);
                } else {
                    this.editNewDocument();
                }
            } else {
                this.queryMode.queryResultList().getNthItem(this.queryMode.currentQueriedItemIndex).done((doc) => this.document(doc));
                this.loadedDocumentName("");
            }

            this.documentChangeNotificationMessage("");
        });
    }

    deleteDocument() {
        var doc = this.document();
        if (doc) {
            var viewModel = new deleteDocuments([doc]);
            viewModel.deletionTask.done(() => {
                this.dirtyFlag().reset(); //Resync Changes

                var list = this.docMode.docsList();
                if (!!list) {
                    this.docMode.docsList().invalidateCache();

                    var newTotalResultCount = list.totalResultCount() - 1;
                    list.totalResultCount(newTotalResultCount);

                    var nextIndex = list.currentItemIndex();
                    if (nextIndex >= newTotalResultCount) {
                        nextIndex = 0;
                    }

                    if (newTotalResultCount > 0) {
                        this.pageToItem(nextIndex, newTotalResultCount);
                    } else {
                        router.navigate(appUrl.forDocuments(null, this.activeDatabase()));
                    }
                }
            });
            app.showDialog(viewModel, editDocument.editDocSelector);
        } 
    }

    formatDocument() {
        try {
            var docEditorText = this.docEditor.getSession().getValue();
            var tempDoc = JSON.parse(docEditorText);
            var formatted = this.stringify(tempDoc);
            this.documentText(formatted);
        } catch (e) {
            messagePublisher.reportError("Could not format json", undefined, undefined, false);
        }
    }

    pageToItem(index: number, newTotalResultCount?: number) {
        var canContinue = this.canContinueIfNotDirty("Unsaved Data", "You have unsaved data. Are you sure you want to continue?");
        canContinue.done(() => {
            var list = this.docMode.docsList();
            if (list) {
                list.getNthItem(index)
                    .done((doc: document) => {
                        if (this.isInDocMode()) {
                            var docId = doc.getId();
                            this.loadDocument(docId).done(() => {
                                this.changeNotification.off();
                                this.removeNotification(this.changeNotification);

                                //TODO: this.changeNotification = this.createDocumentChangeNotification(docId);
                                this.addNotification(this.changeNotification);
                            });
                            list.currentItemIndex(index);
                            this.updateUrl(docId);
                        } else {
                            this.document(doc);
                            this.loadedDocumentName("");
                            this.dirtyFlag().reset(); //Resync Changes
                        }

                        this.documentChangeNotificationMessage("");

                        if (!!newTotalResultCount) {
                            list.totalResultCount(newTotalResultCount);
                        }
                    });
            }
        });
    }

    navigateToCollection(collectionName: string) {
        var collectionUrl = appUrl.forDocuments(collectionName, this.activeDatabase());
        router.navigate(collectionUrl);
    }

    navigateToDocuments() {
        this.navigateToCollection(null);
    }

    updateUrl(docId: string) {
        var docsList = this.docMode.docsList();
        var collectionName = docsList ? docsList.collectionName : null;
        var currentItemIndex = docsList ? docsList.currentItemIndex() : null;
        var editDocUrl = appUrl.forEditDoc(docId, collectionName, currentItemIndex, this.activeDatabase());
        router.navigate(editDocUrl, false);
    }

    metadataChanged(meta: documentMetadata) {
        if (meta) {
            this.metaPropsToRestoreOnSave.length = 0;
            var metaDto = this.metadata().toDto();

            documentMetadata.filterMetadata(metaDto, this.metaPropsToRestoreOnSave);

            if (meta.id != undefined) {
                this.userSpecifiedId(meta.id);
            }
        }
    }

    loadRelatedDocumentsList(document: documentBase) {
        let relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(document);
        let docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.activeDatabase(), true, true);
        let response = docIDsVerifyCommand.execute();
        response.done((verifiedIDs: Array<string>) => {
            this.relatedDocumentHrefs(verifiedIDs.map((verified: string) => {
                return {
                    id: verified.toString(),
                    href: appUrl.forEditDoc(verified.toString(), null, null, this.activeDatabase())
                };
            }));
        });
    }

    resolveConflicts() {
        var task = new resolveMergeCommand(this.activeDatabase(), this.editedDocId()).execute();
        task.done((response: mergeResult) => {
            this.documentText(response.Document);
            //TODO:this.metadataText(response.Metadata);
        });
    }

    getColorClass(documentId: string) {
        var entityName = document.getEntityNameFromId(documentId);
        if (entityName) {
            return collection.getCollectionCssClass(entityName, this.activeDatabase());
        }

        return "";
    }

    generateCollectionName(ravenEntityName: string) { //TODO: move to utils
        return ravenEntityName || (this.isSystemDocumentByDocTitle() ? 'System Documents' : 'No Collection');    
    }

    generateCode() { //TODO: move to helper
        var doc: document = this.document();
        var generate = new generateClassCommand(this.activeDatabase(), doc.getId(), "csharp");
        var deffered = generate.execute();
        deffered.done((code: any) => {
            app.showDialog(new showDataDialog("Generated Class", code["Code"]));
        });
    }
}

export = editDocument;