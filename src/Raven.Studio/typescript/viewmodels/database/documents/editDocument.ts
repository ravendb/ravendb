import app = require("durandal/app");
import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import documentMetadata = require("models/database/documents/documentMetadata");
import collection = require("models/database/documents/collection");
import querySort = require("models/database/query/querySort");

import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import resolveMergeCommand = require("commands/database/studio/resolveMergeCommand");
import getDocumentsFromCollectionCommand = require("commands/database/documents/getDocumentsFromCollectionCommand");
import generateClassCommand = require("commands/database/documents/generateClassCommand");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import pagedResultSet = require("common/pagedResultSet");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import genUtils = require("common/generalUtils");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import documentHelpers = require("common/helpers/database/documentHelpers");
import copyToClipboard = require("common/copyToClipboard");

import deleteDocuments = require("viewmodels/common/deleteDocuments");
import viewModelBase = require("viewmodels/viewModelBase");
import showDataDialog = require("viewmodels/common/showDataDialog");
import connectedDocuments = require("viewmodels/database/documents/editDocumentConnectedDocuments");

class editDocument extends viewModelBase {

    static editDocSelector = "#editDocumentContainer";
    static documentNameSelector = "#documentName";
    static docEditorSelector = "#docEditor";

    document = ko.observable<document>();
    documentText = ko.observable("");
    metadata: KnockoutComputed<documentMetadata>;

    isCreatingNewDocument = ko.observable(false);
    collectionForNewDocument = ko.observable<string>();
    provideCustomNameForNewDocument = ko.observable(false);
    userSpecifiedId = ko.observable("");
    userSpecifiedIdCustomValidityError: KnockoutComputed<string>;
    docEditor: AceAjax.Editor;
    entityName = ko.observable<string>("");

    $documentName: JQuery;
    $docEditor: JQuery;

    isConflictDocument = ko.observable<boolean>(false);
    isInDocMode = ko.observable<boolean>(true);
    isNewLineFriendlyMode = ko.observable<boolean>(false);
    autoCollapseMode = ko.observable<boolean>(false);
    isSaving = ko.observable<boolean>(false);
    displayDocumentChangeNotification = ko.observable<boolean>(false);
    
    metaPropsToRestoreOnSave: any[] = [];

    changeNotification: changeSubscription;
    databaseForEditedDoc: database;

    queryMode = {
        queryIndex: ko.observable<string>(),
        queryResultList: ko.observable<pagedList>(),
        currentQueriedItemIndex: undefined as number    
    };

    connectedDocuments = new connectedDocuments(this.document, this.activeDatabase, (docId) => this.loadDocument(docId));
    
    isSaveEnabled: KnockoutComputed<boolean>;
    documentSize: KnockoutComputed<string>;
    editedDocId: KnockoutComputed<string>;
    
    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.initializeObservables();
    }

    canActivate(args: any) {
        super.canActivate(args);

        if (args && args.id) {
            return this.activateById(args.id);
        } else if (args && args.index) {
            return this.activateByIndex(args.index, args.query, args.sorts, args.item);
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(navigationArgs: { list: string, database:string, item: string, id: string, new: string }) {
        super.activate(navigationArgs);
        this.updateHelpLink('M72H1R');

        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.userSpecifiedId], false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => this.dirtyFlag().isDirty());

        // Find the database and collection we're supposed to load.
        // Used for paging through items.
        this.databaseForEditedDoc = this.activeDatabase();
        if (navigationArgs && navigationArgs.database) {
            ko.postbox.publish("ActivateDatabaseWithName", navigationArgs.database);
        }

        if (navigationArgs && navigationArgs.id) {
            ko.postbox.publish("SetRawJSONUrl", appUrl.forDocumentRawData(this.activeDatabase(), navigationArgs.id)); 
        } else {
            return this.editNewDocument(navigationArgs ? navigationArgs.new : null);
        }
    }

    // Called when the view is attached to the DOM.
    attached() {
        super.attached();
        this.setupKeyboardShortcuts();

        this.$documentName = $(editDocument.documentNameSelector);
        this.$docEditor = $(editDocument.docEditorSelector);

        this.isNewLineFriendlyMode.subscribe(val => {
            this.updateNewlineLayoutInDocument(val);
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.docEditor = aceEditorBindingHandler.getEditorBySelection(this.$docEditor);

        // preload json newline friendly mode to avoid issues with document save
        (<any>ace).config.loadModule("ace/mode/json_newline_friendly");

        this.$docEditor.on('DynamicHeightSet', () => this.docEditor.resize());
        this.focusOnEditor();
    }

    detached() {
        super.detached();
        this.$docEditor.off('DynamicHeightSet');
    }

    private activateById(id: string) {
        var canActivateResult = $.Deferred<canActivateResultDto>();
        this.databaseForEditedDoc = appUrl.getDatabase();
        this.loadDocument(id)
            .done(() => {
                //TODO:this.changeNotification = this.createDocumentChangeNotification(id);
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
        var canActivateResult = $.Deferred<canActivateResultDto>();
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

        this.userSpecifiedIdCustomValidityError = ko.computed(() => {
            var documentId = this.userSpecifiedId();
            return documentId.contains("\\") ? "Document name cannot contain '\\'" : "";
        });

        this.document.subscribe(doc => {
            if (doc) {
                if (this.isConflictDocument()) {
                    this.resolveConflicts();
                } else {

                    const docDto = doc.toDto(true);
                    const metaDto = docDto["@metadata"];
                    if (metaDto) {
                        this.metaPropsToRestoreOnSave.length = 0;

                        documentMetadata.filterMetadata(metaDto, this.metaPropsToRestoreOnSave);
                    }

                    var docText = this.stringify(docDto);
                    this.documentText(docText);
                }
            }
        });

        this.metadata = ko.pureComputed<documentMetadata>(() => this.document() ? this.document().__metadata : null);

        this.isConflictDocument = ko.computed(() => {
            var metadata = this.metadata();
            return metadata && ("Raven-Replication-Conflict" in metadata) && !metadata.id.contains("/conflicts/");
        });

        this.documentSize = ko.pureComputed(() => {
            try {
                var size: number = this.documentText().getSizeInBytesAsUTF8() / 1024;
                return genUtils.formatAsCommaSeperatedString(size, 2);
            } catch (e) {
                return "cannot compute";
            }
        });

        this.metadata.subscribe((meta: documentMetadata) => {
            if (meta.id) {
                this.userSpecifiedId(meta.id);
            }

            this.entityName(document.getEntityNameFromId(meta.id));
        });
        this.editedDocId = ko.pureComputed(() => this.metadata() ? this.metadata().id : "");
        this.editedDocId.subscribe((docId: string) =>
            ko.postbox.publish("SetRawJSONUrl", docId ? appUrl.forDocumentRawData(this.activeDatabase(), docId) : "")
        );
    }

    enableCustomNameProvider() {
        this.provideCustomNameForNewDocument(true);
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

        this.displayDocumentChangeNotification(true);
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

    editNewDocument(collectionForNewDocument: string): JQueryPromise<document> {
        this.isCreatingNewDocument(true);
        this.collectionForNewDocument(collectionForNewDocument);
        
        var documentTask = $.Deferred<document>();

        if (collectionForNewDocument) {
            this.userSpecifiedId(collectionForNewDocument + "/");
            new getDocumentsFromCollectionCommand(new collection(collectionForNewDocument, this.activeDatabase()), 0, 3)
                .execute()
                .done((documents: pagedResultSet<document>) => {
                    var schemaDoc = documentHelpers.findSchema(documents.items);
                    this.document(schemaDoc); 
                    documentTask.resolve(schemaDoc);
                })
                .fail(() => documentTask.reject());
        } else {
            let doc = document.empty();
            this.document(doc);
            documentTask.resolve(doc);
        }

        return documentTask;
    }

    toClipboard() {
        copyToClipboard.copy(this.documentText(), "Document has been copied to clipboard");
    }

    toggleNewlineMode() {
        if (this.isNewLineFriendlyMode() === false && parseInt(this.documentSize().replace(",", "")) > 1024) {
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
        const AceRange = require("ace/range").Range;
        this.docEditor.getSession().foldAll();
        const folds = <any[]> this.docEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.docEditor.getSession().getLength(), 0));
        folds.map(f => this.docEditor.getSession().expandFold(f));
    }

    saveAsNew() {
        var docId = this.userSpecifiedId();
        var slashPosition = docId.indexOf("/", 0);
        if (slashPosition !== -1) {
            docId = docId.substr(0, slashPosition + 1);
        }
        this.saveInternal(docId);
    }

    saveDocument() {
        this.saveInternal(this.userSpecifiedId());
    }

    private saveInternal(documentId: string) {
        this.isInDocMode(true);

        let message = "";
        var updatedDto: any;

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
        
        if (message) {
            messagePublisher.reportError(message, undefined, undefined, false);
            return;
        }

        if (!updatedDto['@metadata']) {
            updatedDto["@metadata"] = {};
        }

        let meta = updatedDto['@metadata'];

        // Fix up the metadata: if we're a new doc, attach the expected reserved properties like ID, ETag, and RavenEntityName.
        // AFAICT, Raven requires these reserved meta properties in order for the doc to be seen as a member of a collection.
        if (this.isCreatingNewDocument()) {
            this.attachReservedMetaProperties(documentId, meta);
        } else {
            // If we're editing a document, we hide some reserved properties from the user.
            // Restore these before we save.
            this.metaPropsToRestoreOnSave.forEach(p => {
                if (p.name !== "Origin") {
                    meta[p.name] = p.value;
                }
            });
            // force document id to support save as new
            meta['@id'] = documentId;
        }

        // skip some not necessary meta in headers
        var metaToSkipInHeaders = ['Raven-Replication-History'];
        for (var i in metaToSkipInHeaders) {
            var skippedHeader = metaToSkipInHeaders[i];
            delete meta[skippedHeader];
        }

        var newDoc = new document(updatedDto);
        var saveCommand = new saveDocumentCommand(documentId, newDoc, this.activeDatabase());
        this.isSaving(true);
        saveCommand
            .execute()
            .done((saveResult: bulkDocumentDto[]) => this.onDocumentSaved(saveResult));
    }

    private onDocumentSaved(saveResult: bulkDocumentDto[]) {
        var savedDocumentDto: bulkDocumentDto = saveResult[0];
        var currentSelection = this.docEditor.getSelectionRange();
        this.loadDocument(savedDocumentDto.Key)
            .always(() => {
                this.updateNewlineLayoutInDocument(this.isNewLineFriendlyMode());

                // Try to restore the selection.
                this.docEditor.selection.setRange(currentSelection, false);
                this.isSaving(false);
            });
        this.updateUrl(savedDocumentDto.Key);

        this.dirtyFlag().reset(); //Resync Changes

        this.isCreatingNewDocument(false);
        this.collectionForNewDocument(null);
    }

    private attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = "0";
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
                this.dirtyFlag().reset();

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
                var docId = this.editedDocId();
                this.document(null);
                this.documentText(null);
                this.userSpecifiedId("");
                this.loadDocument(docId);
            } else {
                this.queryMode.queryResultList().getNthItem(this.queryMode.currentQueriedItemIndex).done((doc) => this.document(doc));
            }

            this.displayDocumentChangeNotification(false);
        });
    }

    deleteDocument() {
        var doc = this.document();
        if (doc) {
            var viewModel = new deleteDocuments([doc]);
            viewModel.deletionTask.done(() => this.connectedDocuments.onDocumentDeleted());
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

    navigateToCollection(collectionName: string) {
        const collectionUrl = appUrl.forDocuments(collectionName, this.activeDatabase());
        router.navigate(collectionUrl);
    }

    updateUrl(docId: string) {
        const editDocUrl = appUrl.forEditDoc(docId, this.activeDatabase());
        router.navigate(editDocUrl, false);
    }

    resolveConflicts() {
        var task = new resolveMergeCommand(this.activeDatabase(), this.editedDocId()).execute();
        task.done((response: mergeResult) => {
            this.documentText(response.Document);
            //TODO: handle metadata
        });
    }

    generateCode() {
        const doc: document = this.document();
        const generate = new generateClassCommand(this.activeDatabase(), doc.getId(), "csharp");
        const deffered = generate.execute();
        deffered.done((code: any) => {
            app.showDialog(new showDataDialog("Generated Class", code["Code"]));
        });
    }
}

export = editDocument;