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

import eventsCollector = require("common/eventsCollector");

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
    userIdHasFocus = ko.observable<boolean>(false);
    userSpecifiedId = ko.observable("");
    userSpecifiedIdCustomValidityError: KnockoutComputed<string>;
    private docEditor: AceAjax.Editor;
    entityName = ko.observable<string>("");

    $documentName: JQuery;
    $docEditor: JQuery;

    isConflictDocument = ko.observable<boolean>(false);
    isInDocMode = ko.observable<boolean>(true);
    isNewLineFriendlyMode = ko.observable<boolean>(false);
    autoCollapseMode = ko.observable<boolean>(false);
    isSaving = ko.observable<boolean>(false);
    displayDocumentChangeNotification = ko.observable<boolean>(false);
    
    private metaPropsToRestoreOnSave: any[] = [];

    private changeNotification: changeSubscription;

    queryMode = {
        queryIndex: ko.observable<string>(),
        queryResultList: ko.observable<pagedList>(),
        currentQueriedItemIndex: undefined as number    
    };

    connectedDocuments = new connectedDocuments(this.document, this.activeDatabase, (docId) => this.loadDocument(docId));
    
    isSaveEnabled: KnockoutComputed<boolean>;
    documentSize: KnockoutComputed<string>;
    editedDocId: KnockoutComputed<string>;
    displayLastModifiedDate: KnockoutComputed<boolean>;
    
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

    activate(navigationArgs: { list: string, database: string, item: string, id: string, new: string }) {
        super.activate(navigationArgs);
        this.updateHelpLink('M72H1R');

        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.userSpecifiedId], false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => this.dirtyFlag().isDirty());

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
        (ace as any).config.loadModule("ace/mode/json_newline_friendly");

        this.$docEditor.on('DynamicHeightSet', () => this.docEditor.resize());
        this.focusOnEditor();
    }

    detached() {
        super.detached();
        this.$docEditor.off('DynamicHeightSet');
    }

    private activateById(id: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();
        this.loadDocument(id)
            .done(() => {
                this.changeNotification = this.createDocumentChangeNotification(id);
                this.addNotification(this.changeNotification);
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                messagePublisher.reportError(`Could not find ${id} document`);
                canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocsCollectionName, this.activeDatabase()) });
            });
        return canActivateResult;
    }

    private activateByIndex(indexName: string, query: string, argSorts: string, argItem: any) {
        const canActivateResult = $.Deferred<canActivateResultDto>();
        this.isInDocMode(false);
        let sorts: querySort[];

        if (argSorts) {
            sorts = argSorts.split(",").map((curSort: string) => querySort.fromQuerySortString(curSort.trim()));
        } else {
            sorts = [];
        }

        const resultsFetcher = (skip: number, take: number) => new queryIndexCommand(indexName, this.activeDatabase(), skip, take, query, sorts).execute();
        const list = new pagedList(resultsFetcher);
        const item = !!argItem && !isNaN(argItem) ? argItem : 0;

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
            const documentId = this.userSpecifiedId();
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

                    const docText = this.stringify(docDto);
                    this.documentText(docText);
                }
            }
        });

        this.metadata = ko.pureComputed<documentMetadata>(() => this.document() ? this.document().__metadata : null);

        this.isConflictDocument = ko.computed(() => {
            const metadata = this.metadata();
            return metadata && ("Raven-Replication-Conflict" in metadata) && !metadata.id.contains("/conflicts/");
        });

        this.documentSize = ko.pureComputed(() => {
            try {
                const size: number = this.documentText().getSizeInBytesAsUTF8() / 1024;
                return genUtils.formatAsCommaSeperatedString(size, 2);
            } catch (e) {
                return "cannot compute";
            }
        });

        this.metadata.subscribe((meta: documentMetadata) => {
            if (meta && meta.id) {
                this.userSpecifiedId(meta.id);
                this.entityName(document.getEntityNameFromId(meta.id));
            }
        });
        this.editedDocId = ko.pureComputed(() => this.metadata() ? this.metadata().id : "");
        this.editedDocId.subscribe((docId: string) =>
            ko.postbox.publish("SetRawJSONUrl", docId ? appUrl.forDocumentRawData(this.activeDatabase(), docId) : "")
        );
        this.displayLastModifiedDate = ko.pureComputed<boolean>(() => {
            const hasMetadata = !!this.metadata();
            const inEditMode = !this.isCreatingNewDocument();
            const displayChangedNotification = this.displayDocumentChangeNotification();

            return hasMetadata && inEditMode && !displayChangedNotification;
        });
    }

    enableCustomNameProvider() {
        this.provideCustomNameForNewDocument(true);
        this.userIdHasFocus(true);
    }

    createDocumentChangeNotification(docId: string): changeSubscription {
        return this.changesContext.currentResourceChangesApi().watchDocument(docId, (n: Raven.Abstractions.Data.DocumentChangeNotification) => this.documentChangeNotification(n));
    }

    documentChangeNotification(n: Raven.Abstractions.Data.DocumentChangeNotification): void {
        if (this.isSaving() || n.Etag === this.metadata().etag) {
            return;
        }

        this.displayDocumentChangeNotification(true);
    }

    updateNewlineLayoutInDocument(unescapeNewline: boolean) {
        const dirtyFlagValue = this.dirtyFlag().isDirty();
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
        
        const documentTask = $.Deferred<document>();

        if (collectionForNewDocument) {
            this.userSpecifiedId(collectionForNewDocument + "/");
            new getDocumentsFromCollectionCommand(new collection(collectionForNewDocument, this.activeDatabase()), 0, 3)
                .execute()
                .done((documents: pagedResultSet<document>) => {
                    const schemaDoc = documentHelpers.findSchema(documents.items);
                    this.document(schemaDoc); 
                    documentTask.resolve(schemaDoc);
                })
                .fail(() => documentTask.reject());
        } else {
            const doc = document.empty();
            this.document(doc);
            documentTask.resolve(doc);
        }

        return documentTask;
    }

    toClipboard() {
        copyToClipboard.copy(this.documentText(), "Document has been copied to clipboard");
    }

    toggleNewlineMode() {
        eventsCollector.default.reportEvent("document", "toggle-newline-mode");
        if (this.isNewLineFriendlyMode() === false && parseInt(this.documentSize().replace(",", "")) > 1024) {
            app.showBootstrapMessage("This operation might take long time with big documents, are you sure you want to continue?", "Toggle newline mode", ["Cancel", "Continue"])
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
        eventsCollector.default.reportEvent("document", "toggle-auto-collapse");
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
        let docId = this.userSpecifiedId();
        const slashPosition = docId.indexOf("/", 0);
        if (slashPosition !== -1) {
            docId = docId.substr(0, slashPosition + 1);
        }
        this.saveInternal(docId, true);
    }

    saveDocument() {
        eventsCollector.default.reportEvent("document", "save");
        this.saveInternal(this.userSpecifiedId(), false);
    }

    private saveInternal(documentId: string, eraseEtag: boolean) {
        this.isInDocMode(true);

        let message = "";
        let updatedDto: any;

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

        const meta = updatedDto['@metadata'];

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

            if (eraseEtag) {
                meta['@etag'] = 0;
            }
        }

        // skip some not necessary meta in headers
        const metaToSkipInHeaders = ['Raven-Replication-History'];
        for (let i in metaToSkipInHeaders) {
            const skippedHeader = metaToSkipInHeaders[i];
            delete meta[skippedHeader];
        }

        const newDoc = new document(updatedDto);
        const saveCommand = new saveDocumentCommand(documentId, newDoc, this.activeDatabase());
        this.isSaving(true);
        saveCommand
            .execute()
            .done((saveResult: saveDocumentResponseDto) => this.onDocumentSaved(saveResult));
    }

    private onDocumentSaved(saveResult: saveDocumentResponseDto) {
        const savedDocumentDto: saveDocumentResponseItemDto = saveResult.Results[0];
        const currentSelection = this.docEditor.getSelectionRange();
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
        target['@etag'] = 0;
        target['Raven-Entity-Name'] = target['Raven-Entity-Name'] || document.getEntityNameFromId(id);
        target['@id'] = id;
    }

    stringify(obj: any) {
        const prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    loadDocument(id: string): JQueryPromise<document> {
        this.isBusy(true);

        return new getDocumentWithMetadataCommand(id, this.activeDatabase())
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
        eventsCollector.default.reportEvent("document", "refresh");
        this.canContinueIfNotDirty("Refresh", "You have unsaved data. Are you sure you want to continue?")
        .done(() => {
            if (this.isInDocMode()) {
                const docId = this.editedDocId();
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
        eventsCollector.default.reportEvent("document", "delete");
        const doc = this.document();
        if (doc) {
            const viewModel = new deleteDocuments([doc], this.activeDatabase());
            viewModel.deletionTask.done(() => this.connectedDocuments.onDocumentDeleted());
            app.showBootstrapDialog(viewModel, editDocument.editDocSelector);
        } 
    }

    formatDocument() {
        eventsCollector.default.reportEvent("document", "format");
        try {
            const docEditorText = this.docEditor.getSession().getValue();
            const tempDoc = JSON.parse(docEditorText);
            const formatted = this.stringify(tempDoc);
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
        eventsCollector.default.reportEvent("document", "resolve-conflicts");
        new resolveMergeCommand(this.activeDatabase(), this.editedDocId())
            .execute()
            .done((response: mergeResult) => {
                this.documentText(response.Document);
                //TODO: handle metadata
            });
    }

    generateCode() {
        eventsCollector.default.reportEvent("document", "generate-csharp-code");
        const doc: document = this.document();
        const generate = new generateClassCommand(this.activeDatabase(), doc.getId(), "csharp");
        const deffered = generate.execute();
        deffered.done((code: string) => {
            app.showBootstrapDialog(new showDataDialog("Generated Class", code));
        });
    }
}

export = editDocument;