import app = require("durandal/app");
import router = require("plugins/router");

import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import collection = require("models/database/documents/collection");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import cloneAttachmentsAndCountersCommand = require("commands/database/documents/cloneAttachmentsAndCountersCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getDocumentPhysicalSizeCommand = require("commands/database/documents/getDocumentPhysicalSizeCommand");
import getDocumentsFromCollectionCommand = require("commands/database/documents/getDocumentsFromCollectionCommand");
import getRevisionsBinDocumentMetadataCommand = require("commands/database/documents/getRevisionsBinDocumentMetadataCommand");
import generateClassCommand = require("commands/database/documents/generateClassCommand");
import getCountersCommand = require("commands/database/documents/counters/getCountersCommand");
import setCounterDialog = require("viewmodels/database/documents/setCounterDialog");
import appUrl = require("common/appUrl");
import viewHelpers = require("common/helpers/view/viewHelpers");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import deleteCounterCommand = require("commands/database/documents/counters/deleteCounterCommand");
import genUtils = require("common/generalUtils");
import changeSubscription = require("common/changeSubscription");
import documentHelpers = require("common/helpers/database/documentHelpers");
import copyToClipboard = require("common/copyToClipboard");
import deleteAttachmentCommand = require("commands/database/documents/attachments/deleteAttachmentCommand");
import setCounterCommand = require("commands/database/documents/counters/setCounterCommand");
import CountersDetail = Raven.Client.Documents.Operations.Counters.CountersDetail;

import deleteDocuments = require("viewmodels/common/deleteDocuments");
import viewModelBase = require("viewmodels/viewModelBase");
import showDataDialog = require("viewmodels/common/showDataDialog");
import connectedDocuments = require("viewmodels/database/documents/editDocumentConnectedDocuments");
import getDocumentAtRevisionCommand = require("commands/database/documents/getDocumentAtRevisionCommand");
import changeVectorUtils = require("common/changeVectorUtils");

import eventsCollector = require("common/eventsCollector");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import database = require("models/resources/database");

class editDocument extends viewModelBase {

    static editDocSelector = "#editDocumentContainer";
    static documentNameSelector = "#documentName";
    static docEditorSelector = "#docEditor";

    inReadOnlyMode = ko.observable<boolean>(false);
    revisionChangeVector = ko.observable<string>();
    document = ko.observable<document>();
    documentText = ko.observable("");
    metadata: KnockoutComputed<documentMetadata>;
    
    changeVector: KnockoutComputed<changeVectorItem[]>;
    changeVectorHtml: KnockoutComputed<string>;
    
    lastModifiedAsAgo: KnockoutComputed<string>;    
    latestRevisionUrl: KnockoutComputed<string>;
    rawJsonUrl: KnockoutComputed<string>;
    isDeleteRevision: KnockoutComputed<boolean>;

    isCreatingNewDocument = ko.observable(false);
    isClone = ko.observable(false);
    collectionForNewDocument = ko.observable<string>();
    provideCustomNameForNewDocument = ko.observable(false);
    userIdHasFocus = ko.observable<boolean>(false);   
    userSpecifiedId = ko.observable<string>("");

    globalValidationGroup = ko.validatedObservable({
        userDocumentId: this.userSpecifiedId,
        userDocumentText: this.documentText
    });
    
    documentExpirationEnabled: KnockoutComputed<boolean>;

    private docEditor: AceAjax.Editor;
    entityName = ko.observable<string>("");

    $documentName: JQuery;
    $docEditor: JQuery;

    isConflictDocument = ko.observable<boolean>(false);
    isNewLineFriendlyMode = ko.observable<boolean>(false);
    autoCollapseMode = ko.observable<boolean>(false);
    isSaving = ko.observable<boolean>(false);
    displayDocumentChange = ko.observable<boolean>(false);
    
    private metaPropsToRestoreOnSave: any[] = [];

    private changeNotification: changeSubscription;

    private normalActionProvider = new normalCrudActions(this.document, this.activeDatabase, 
            docId => this.loadDocument(docId), (saveResult: saveDocumentResponseDto, localDoc: any) => this.onDocumentSaved(saveResult, localDoc));
    
    // it represents effective actions provider - normally it uses normalActionProvider, in clone document node it overrides actions on attachments/counter to 'in-memory' implementation 
    private crudActionsProvider = ko.observable<editDocumentCrudActions>(this.normalActionProvider); 

    connectedDocuments = new connectedDocuments(this.document, this.activeDatabase, (docId) => this.loadDocument(docId), this.isCreatingNewDocument, this.crudActionsProvider, this.inReadOnlyMode);

    isSaveEnabled: KnockoutComputed<boolean>;
    
    computedDocumentSize: KnockoutComputed<string>;
    sizeOnDiskActual = ko.observable<string>();
    sizeOnDiskAllocated = ko.observable<String>();
    documentSizeHtml: KnockoutComputed<string>;
    
    editedDocId: KnockoutComputed<string>;
    displayLastModifiedDate: KnockoutComputed<boolean>;
    collectionTracker = collectionsTracker.default;

    canViewAttachments: KnockoutComputed<boolean>;
    canViewCounters: KnockoutComputed<boolean>;
    canViewRelated: KnockoutComputed<boolean>;
    
    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.initializeObservables();
        this.initValidation();
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                if (args && args.revisionBinEntry && args.id) {
                    return this.activateByRevisionsBinEntry(args.id);
                } else if (args && args.id && args.revision) {
                    return this.activateByRevision(args.id, args.revision);
                } else if (args && args.id) {
                    return this.activateById(args.id);
                } else {
                    return $.Deferred().resolve({ can: true });
                }
            });
    }

    activate(navigationArgs: { list: string, database: string, item: string, id: string, new: string, index: string, revision: number }) {
        super.activate(navigationArgs);
        this.updateHelpLink('M72H1R');

        //TODO: raw url for revision
        if (!navigationArgs || !navigationArgs.id) {
            return this.editNewDocument(navigationArgs ? navigationArgs.new : null);
        }

        this.setActiveTab();
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
        (ace as any).config.loadModule("ace/mode/raven_document_newline_friendly");

        this.connectedDocuments.compositionComplete();

        this.focusOnEditor();
       
        $('#right-options-panel [data-toggle="tooltip"]').tooltip(); 
    }

    detached() {
        super.detached();
        this.connectedDocuments.dispose();
    }

    private activateById(id: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();
        this.loadDocument(id)
            .done(() => {
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocumentsCollectionName, this.activeDatabase()) });
            });
        return canActivateResult;
    }

    private activateByRevisionsBinEntry(id: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();

        this.loadRevisionsBinEntry(id)
            .done(() => canActivateResult.resolve({ can: true }))
            .fail(() => canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocumentsCollectionName, this.activeDatabase()) }));
            
        return canActivateResult;
    }

    private activateByRevision(id: string, revisionChangeVector: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();
        this.loadRevision(revisionChangeVector)
            .done(() => {
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                canActivateResult.resolve({ redirect: appUrl.forEditDoc(id, this.activeDatabase()) });
            });
        return canActivateResult;
    }

    afterClientApiConnected(): void {
        this.syncChangeNotification();
    }

    private syncChangeNotification() {
        if (this.changeNotification) {
            this.removeNotification(this.changeNotification);
            this.changeNotification.off();
        }

        if (!this.document() || this.isCreatingNewDocument())
            return;

        this.changeNotification = this.createDocumentChangeNotification(this.document().getId());
        this.addNotification(this.changeNotification);
    }

    private initValidation() {
        const rg1 = /^[^\\]*$/; // forbidden character - backslash
        this.userSpecifiedId.extend({
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Can't use backslash in document name"
                }]
        });

        this.documentText.extend({
            required: true,
            aceValidation: true,
            validation: [
                {
                    validator: (val: string) => {
                        try {
                            const parsedJson = JSON.parse(val);
                            return _.isPlainObject(parsedJson);
                        } catch {
                            return false;
                        }
                    },
                    message: "Document must be valid JSON object"
                }
            ]
        });
    }

    private initializeObservables(): void {
        
        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.userSpecifiedId], false, jsonUtil.newLineNormalizingHashFunction); 
          
        this.isSaveEnabled = ko.pureComputed(() => {
            const isSaving = this.isSaving();
            const isDirty = this.dirtyFlag().isDirty();
            const changeVector = this.metadata().changeVector();

            if (isSaving || (!isDirty && changeVector)) {
                return false;
            }

            return true;
        });         

        this.rawJsonUrl = ko.pureComputed(() => {
            const newDocMode = this.isCreatingNewDocument();
            if (newDocMode) {
                return null;
            }

            const isRevision = this.inReadOnlyMode();

            const docId = this.userSpecifiedId();
            const revisionChangeVector = this.revisionChangeVector();

            const activeDb = this.activeDatabase();
            if (!activeDb) {
                return null;
            }

            return isRevision ? 
                appUrl.forDocumentRevisionRawData(activeDb, revisionChangeVector) :
                appUrl.forDocumentRawData(activeDb, docId);
        });
        
        this.documentExpirationEnabled = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (db) {
                return db.hasExpirationConfiguration();
            } else {
                return false;
            }
        })

        this.isDeleteRevision = ko.pureComputed(() => {
            const doc = this.document();
            if (doc) {
                return doc.__metadata.hasFlag("DeleteRevision");
            } else {
                return false;
            }
        });

        this.document.subscribe(doc => {
            if (doc) {
                const docDto = doc.toDto(true);
                const metaDto = docDto["@metadata"];
                if (metaDto) {
                    this.metaPropsToRestoreOnSave.length = 0;
                    documentMetadata.filterMetadata(metaDto, this.metaPropsToRestoreOnSave);
                }

                const docText = this.stringify(docDto);
                this.documentText(docText);
            }
        });

        this.metadata = ko.pureComputed<documentMetadata>(() => this.document() ? this.document().__metadata : null);
        
        this.changeVector = ko.pureComputed(() => {
            const meta = this.metadata();
            if (!meta || !meta.changeVector()) {
                return [];
            }
            const vector = meta.changeVector();

            return changeVectorUtils.formatChangeVector(vector, changeVectorUtils.shouldUseLongFormat([vector]));
        });
        
        this.changeVectorHtml = ko.pureComputed(() => {
            let vectorText = "<h4>Change Vector</h4>";
            if (this.changeVector().length) {
                vectorText += this.changeVector().map(vectorItem => vectorItem.fullFormat).join('<br/>');
            }
            
            return vectorText;
        });

        this.isConflictDocument = ko.computed(() => {
            const metadata = this.metadata();
            return metadata && ("Raven-Replication-Conflict" in metadata) && !metadata.id.includes("/conflicts/");
        });

        this.computedDocumentSize = ko.pureComputed(() => {
            try {
                const textSize: number = genUtils.getSizeInBytesAsUTF8(this.documentText());
                const metadataAsString = JSON.stringify(this.metadata().toDto());
                const metadataSize = genUtils.getSizeInBytesAsUTF8(metadataAsString);
                const metadataKey = genUtils.getSizeInBytesAsUTF8(", @metadata: ");
                return genUtils.formatBytesToSize(textSize + metadataSize + metadataKey);
            } catch (e) {
                return "cannot compute";
            }
        });

        this.documentSizeHtml = ko.computed(() => {
            if (this.isClone() || this.isCreatingNewDocument() || this.inReadOnlyMode()) {
                return `Computed Size: ${this.computedDocumentSize()} KB`;
            }
            
            return `<h4>Document Size on Disk</h4> Actual Size: ${this.sizeOnDiskActual()} <br/> Allocated Size: ${this.sizeOnDiskAllocated()}`;            
        });
        
        this.metadata.subscribe((meta: documentMetadata) => {
            if (meta && meta.id) {
                this.userSpecifiedId(meta.id);
                this.entityName(document.getCollectionFromId(meta.id, this.collectionTracker.getCollectionNames()));
            }
        });
        this.editedDocId = ko.pureComputed(() => this.metadata() ? this.metadata().id : "");
       
        this.displayLastModifiedDate = ko.pureComputed<boolean>(() => {
            const hasMetadata = !!this.metadata();
            const inEditMode = !this.isCreatingNewDocument();
            const displayChangedNotification = this.displayDocumentChange();

            return hasMetadata && inEditMode && !displayChangedNotification;
        });

        this.lastModifiedAsAgo = ko.pureComputed(() => {
            const metadata = this.metadata();
            return metadata ? metadata.lastModifiedInterval() : "";
        });

        this.latestRevisionUrl = ko.pureComputed(() => {
            const id = this.document().getId();
            return appUrl.forEditDoc(id, this.activeDatabase());
        });

        this.canViewAttachments = ko.pureComputed(() => {
            if (this.isClone()) {
                return true;
            }
            return !this.connectedDocuments.isArtificialDocument() && !this.connectedDocuments.isHiloDocument() && !this.isCreatingNewDocument() && !this.isDeleteRevision();
        });

        this.canViewCounters = ko.pureComputed(() => {
            if (this.isClone()) {
                return true;
            }
            return !this.connectedDocuments.isArtificialDocument() && !this.connectedDocuments.isHiloDocument() && !this.isCreatingNewDocument() && !this.isDeleteRevision();
        });

        this.canViewRelated = ko.pureComputed(() => {
            return !this.isDeleteRevision();
        });
    }

    enableCustomNameProvider() {
        this.provideCustomNameForNewDocument(true);
        this.userIdHasFocus(true);
    }

    createDocumentChangeNotification(docId: string): changeSubscription {
        return this.changesContext.databaseChangesApi().watchDocument(docId, (n: Raven.Client.Documents.Changes.DocumentChange) => this.onDocumentChange(n));
    }

    onDocumentChange(n: Raven.Client.Documents.Changes.DocumentChange): void {
        if (this.isSaving() || n.ChangeVector === this.metadata().changeVector() || this.inReadOnlyMode()) {
            return;
        }

        this.displayDocumentChange(true);
    }

    updateNewlineLayoutInDocument(unescapeNewline: boolean) {
        const dirtyFlagValue = this.dirtyFlag().isDirty();
        if (unescapeNewline) {
            this.documentText(documentHelpers.unescapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/raven_document_newline_friendly');
        } else {
            this.documentText(documentHelpers.escapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/raven_document');
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
        this.createKeyboardShortcut("alt+s", () => this.saveDocument(), editDocument.editDocSelector); 
        // Q. Why do we have to setup ALT+S, when we could just use HTML's accesskey attribute?
        // A. Because the accesskey attribute causes the save button to take focus, thus stealing the focus from the user's editing spot in the doc editor, disrupting his workflow.
    }

    private focusOnEditor() {
        this.docEditor.focus();
    }

    editNewDocument(collectionForNewDocument: string): JQueryPromise<document> {
        this.isCreatingNewDocument(true);
        this.collectionForNewDocument(collectionForNewDocument);
        
        const documentTask = $.Deferred<document>();

        if (collectionForNewDocument) {
            this.userSpecifiedId(this.defaultNameForNewDocument(collectionForNewDocument));
            new getDocumentsFromCollectionCommand(new collection(collectionForNewDocument, this.activeDatabase()), 0, 3)
                .execute()
                .done((documents: pagedResult<document>) => {
                    const schemaDoc = documentHelpers.findSchema(documents.items);
                    this.document(schemaDoc); 
                    documentTask.resolve(schemaDoc);
                })
                .fail(() => documentTask.reject());
        } else {
            const doc: any = document.empty();
            doc["Name"] = "...";
            this.document(doc);

            documentTask.resolve(doc);
        }

        return documentTask;
    }

    private defaultNameForNewDocument(collectionForNewDocument: string) {
        // We get here upon clicking 'Clone' or 'New doc in current collection'
        // Just append / to collection name if exists
        
        if (!collectionForNewDocument ||  collectionForNewDocument === "@empty") {
            return "";
        }
        
        return collectionForNewDocument + "/";
    }

    toClipboard() {
        copyToClipboard.copy(this.documentText(), "Document has been copied to clipboard");
    }

    toggleNewlineMode() {
        eventsCollector.default.reportEvent("document", "toggle-newline-mode");
        if (this.isNewLineFriendlyMode() === false && parseInt(this.computedDocumentSize().replace(",", "")) > 1024) {
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
        const AceRange = ace.require("ace/range").Range;
        this.docEditor.getSession().foldAll();
        const folds = <any[]> this.docEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.docEditor.getSession().getLength(), 0));
        folds.map(f => this.docEditor.getSession().expandFold(f));
    }

    createClone() {
        const attachments = this.document().__metadata.attachments() 
            ? this.document().__metadata.attachments().map(x => editDocument.mapToAttachmentItem(this.editedDocId(), x))
            : [];
        
        if (this.crudActionsProvider().countersCount() > 0) {
            // looks like we have at least one counter - download current values before doing clone
            this.normalActionProvider.fetchCounters("", 0, 1024 * 1024)
                .then(counters => {
                    this.createCloneInternal(attachments, counters.items);
                })
        } else {
            this.createCloneInternal(attachments, []);
        }
    }
    
    private createCloneInternal(attachments: attachmentItem[], counters: counterItem[]) {
        // Show current document as a new document...
        this.crudActionsProvider(new clonedDocumentCrudActions(this, this.activeDatabase, 
            attachments, counters, () => this.connectedDocuments.reload()));

        this.isCreatingNewDocument(true);
        this.isClone(true);
        this.inReadOnlyMode(false);

        this.syncChangeNotification();

        eventsCollector.default.reportEvent("document", "clone");

        // Remove the '@change-vector' & '@flags' from metadata view for the clone 
        const docDto = this.document().toDto(true);
        const metaDto = docDto["@metadata"];
        let docId = "";

        if (metaDto) {
            documentMetadata.filterMetadata(metaDto, this.metaPropsToRestoreOnSave, true);
            const docText = this.stringify(docDto);
            this.documentText(docText);

            // Suggest initial document Id 
            docId = this.defaultNameForNewDocument(metaDto["@collection"]);
        }

        // Clear data..
        this.document().__metadata.clearFlags();
        this.connectedDocuments.revisionsCount(0);

        this.connectedDocuments.gridController().reset(true);
        this.metadata().changeVector(undefined);

        this.userSpecifiedId(docId);

        this.setActiveTab();
    }

    saveDocument() {
        if (this.isValid(this.globalValidationGroup)) {
            eventsCollector.default.reportEvent("document", "save");
            this.saveInternal(this.userSpecifiedId());
        }
    }

    private saveInternal(documentId: string) {
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

        // Fix up the metadata: if we're a new doc, attach the expected reserved properties like @id and @collection.
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
        const metaToSkipInHeaders = ['Raven-Replication-History'];
        for (let i in metaToSkipInHeaders) {
            const skippedHeader = metaToSkipInHeaders[i];
            delete meta[skippedHeader];
        }

        // we split save of cloned document into 2 calls, as default id convention creates ids like: users/
        // as result we don't know exact destination document id.
        const newDoc = new document(updatedDto);
        const saveCommand = new saveDocumentCommand(documentId, newDoc, this.activeDatabase(), true);
        this.isSaving(true);
        saveCommand
            .execute()
            .then((saveResult: saveDocumentResponseDto) => {
                return this.crudActionsProvider().saveRelatedItems(saveResult.Results[0]["@id"])
                    .then(() => saveResult);
            })
            .then((saveResult: saveDocumentResponseDto) => this.crudActionsProvider().onDocumentSaved(saveResult, updatedDto))
            .fail(() => {
                this.isSaving(false);
            });
    }
    
    private onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any) {
        this.isClone(false);
        this.crudActionsProvider(this.normalActionProvider);
        
        const savedDocumentDto: changedOnlyMetadataFieldsDto = saveResult.Results[0];
        const currentSelection = this.docEditor.getSelectionRange();

        const metadata = localDoc['@metadata'];
        for (let prop in savedDocumentDto) {
            if (savedDocumentDto.hasOwnProperty(prop)) {
                if (prop === "Type")
                    continue;
                if (prop === "@collection" && savedDocumentDto["@collection"] === "@empty")
                    continue;
                metadata[prop] = (savedDocumentDto as any)[prop];
            }
        }

        // server only sends @flags if there are any
        if ("@flags" in metadata && !("@flags" in savedDocumentDto)) {
            delete metadata["@flags"];
        }

        const newDoc = new document(localDoc);
        this.document(newDoc);
        this.inReadOnlyMode(false);
        this.revisionChangeVector(null);
        this.displayDocumentChange(false);
        this.dirtyFlag().reset();

        this.updateNewlineLayoutInDocument(this.isNewLineFriendlyMode());

        // Try to restore the selection.
        this.docEditor.selection.setRange(currentSelection, false);
        this.isSaving(false);
        this.syncChangeNotification();
        this.connectedDocuments.onDocumentSaved();

        this.updateUrl(savedDocumentDto["@id"]);

        this.dirtyFlag().reset(); //Resync Changes

        this.isCreatingNewDocument(false);
        this.collectionForNewDocument(null);

        $('#right-options-panel [data-toggle="tooltip"]').tooltip();
        this.getDocumentPhysicalSize(metadata['@id']);
    }

    private attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        // Define a collection to be sent to server only if there is a relevant value 
        target['@id'] = id;
        target['@collection'] = target['@collection'] || document.getCollectionFromId(id, this.collectionTracker.getCollectionNames());
    }

    stringify(obj: any) {
        const prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    private loadDocument(id: string): JQueryPromise<document> {
        this.isBusy(true);

        const db = this.activeDatabase();
        const loadTask = $.Deferred<document>();

        new getDocumentWithMetadataCommand(id, db)
            .execute()
            .done((doc: document) => {
                this.document(doc);
                this.inReadOnlyMode(false);
                this.displayDocumentChange(false);
                this.dirtyFlag().reset();

                if (this.autoCollapseMode()) {
                    this.foldAll();
                }
                
                this.getDocumentPhysicalSize(id);
                
                loadTask.resolve(doc);
            })
            .fail((xhr: JQueryXHR) => {
                // if revisions is enabled try to load revisions bin entry
                if (xhr.status === 404 && db.hasRevisionsConfiguration()) {
                    this.loadRevisionsBinEntry(id)
                        .done(doc => loadTask.resolve(doc))
                        .fail(() => loadTask.reject());
                } else {
                    this.dirtyFlag().reset();
                    messagePublisher.reportError("Could not find document: " + id);
                    loadTask.reject();
                }
            })
            .always(()=> this.isBusy(false));

        return loadTask;
    }

    private getDocumentPhysicalSize(id: string): JQueryPromise<Raven.Server.Documents.Handlers.DocumentSizeDetails> {
        return new getDocumentPhysicalSizeCommand(id, this.activeDatabase())
            .execute()
            .done((size) => {
                this.sizeOnDiskActual(size.HumaneActualSize);
                this.sizeOnDiskAllocated(size.HumaneAllocatedSize);
            })
            .fail(() => {
                this.sizeOnDiskActual("Failed to get size");
                this.sizeOnDiskAllocated("Failed to get size");
            }); 
    }
    
    private loadRevisionsBinEntry(id: string): JQueryPromise<document> {
        return new getRevisionsBinDocumentMetadataCommand(id, this.activeDatabase())
            .execute()
            .done((doc: document) => {
                this.document(doc);
                this.displayDocumentChange(false);

                this.inReadOnlyMode(true);

                this.dirtyFlag().reset();
                if (this.autoCollapseMode()) {
                    this.foldAll();
                }
            })
            .fail(() => {
                this.dirtyFlag().reset();

                messagePublisher.reportError("Could not find revisions bin entry: " + id);
                router.navigate(appUrl.forDocuments(null, this.activeDatabase()));
            });
    }

    private loadRevision(changeVector: string) : JQueryPromise<document> {
        this.isBusy(true);

        return new getDocumentAtRevisionCommand(changeVector, this.activeDatabase())
            .execute()
            .done((doc: document) => {
                this.document(doc);
                this.displayDocumentChange(false);

                this.inReadOnlyMode(true);
                this.revisionChangeVector(changeVector);

                this.dirtyFlag().reset();

                if (this.autoCollapseMode()) {
                    this.foldAll();
                }
            })
            .fail(() => messagePublisher.reportError("Could not find requested revision. Redirecting to latest version"))
            .always(() => this.isBusy(false));
    }

    refreshDocument() {
        eventsCollector.default.reportEvent("document", "refresh");
        this.canContinueIfNotDirty("Refresh", "You have unsaved data. Are you sure you want to continue?")
            .done(() => {
                const docId = this.editedDocId();
                this.userSpecifiedId("");
                    this.loadDocument(docId)
                        .done(() => {
                            this.connectedDocuments.gridController().reset(true);
                        });

                this.displayDocumentChange(false);
            });
    }

    deleteDocument() {
        eventsCollector.default.reportEvent("document", "delete");
        const doc = this.document();
        if (doc) {
            const viewModel = new deleteDocuments([doc.getId()], this.activeDatabase());
            viewModel.deletionTask.done(() => {
                this.dirtyFlag().reset();
                this.connectedDocuments.onDocumentDeleted();                
            });
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

    generateClass() {
        eventsCollector.default.reportEvent("document", "generate-csharp-class");

        const doc: document = this.document();
        const generate = new generateClassCommand(this.activeDatabase(), doc.getId(), "csharp");
        const deferred = generate.execute();
        deferred.done((code: string) => app.showBootstrapDialog(new showDataDialog("The Generated C# Class", code, "csharp", null)));
    }
    
    private setActiveTab() {
        if (this.isDeleteRevision()) {
            this.connectedDocuments.activateRevisions(true);
        } else if (!this.canViewAttachments() || !this.canViewCounters()) {
            this.connectedDocuments.activateRecent();
        } else if (this.inReadOnlyMode()) { // revision mostly..
            this.connectedDocuments.activateRevisions(false);
        } else {
            this.connectedDocuments.activateAttachments();
        }
    }

    static mapToAttachmentItem(documentId: string, file: documentAttachmentDto): attachmentItem {
        return {
            documentId: documentId,
            name: file.Name,
            contentType: file.ContentType,
            size: file.Size
        } as attachmentItem;
    }

}

class normalCrudActions implements editDocumentCrudActions {
    
    private readonly document: KnockoutObservable<document>;
    private readonly db: KnockoutObservable<database>;
    private readonly loadDocument: (id: string) => JQueryPromise<document>;
    private readonly onDocumentSavedAction: (saveResult: saveDocumentResponseDto, localDoc: any) => void | JQueryPromise<void>;

    attachmentsCount: KnockoutComputed<number>;
    countersCount: KnockoutComputed<number>;

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, loadDocument: (id: string) => JQueryPromise<document>,
                onDocumentSaved: (saveResult: saveDocumentResponseDto, localDoc: any) => void | JQueryPromise<void>) {
        this.document = document;
        this.db = db;
        this.loadDocument = loadDocument;
        this.onDocumentSavedAction = onDocumentSaved;
        
        _.bindAll(this, "setCounter");

        this.attachmentsCount = ko.pureComputed(() => {
            const doc = this.document();
            if (!doc || !doc.__metadata || !doc.__metadata.attachments()) {
                return 0;
            }

            return doc.__metadata.attachments().length;
        });

        this.countersCount = ko.pureComputed(() => {
            const doc = this.document();

            if (doc && doc.__metadata && doc.__metadata.revisionCounters().length) {
                return doc.__metadata.revisionCounters().length;
            }

            if (!doc || !doc.__metadata || !doc.__metadata.counters()) {
                return 0;
            }

            return doc.__metadata.counters().length;
        });
    }
    
    setCounter(counter: counterItem) {
        const saveAction = (newCounter: boolean, counterName: string, newValue: number, 
            db: database, onCounterNameError: (error: string) => void): JQueryPromise<CountersDetail> => {
            const documentId = this.document().getId();
            
            const saveTask = () => {
                const previousValue = counter ? counter.totalCounterValue : 0;
                const counterDeltaValue = newValue - previousValue;
                return new setCounterCommand(counterName, counterDeltaValue, documentId, db)
                    .execute()
                    .done(() => {
                        this.loadDocument(this.document().getId());
                    })
            };
    
            if (newCounter) {
                return new getCountersCommand(documentId, db)
                    .execute()
                    .then((counters: Raven.Client.Documents.Operations.Counters.CountersDetail) => {
                        if (counters.Counters.find(x => x.CounterName === counterName)) {
                            const error = "Counter '" + counterName + "' already exists.";
                            onCounterNameError(error);
                            return $.Deferred<CountersDetail>().reject(error);
                        } else {
                            return saveTask();
                        }
                    })
            } else {
                return saveTask();
            }
        };
        
        eventsCollector.default.reportEvent("counters", "set");
        const setCounterView = new setCounterDialog(counter, this.db(), true, saveAction);

        app.showBootstrapDialog(setCounterView);
    }

    deleteAttachment(file: attachmentItem) {
        eventsCollector.default.reportEvent("attachments", "delete");
        viewHelpers.confirmationMessage("Delete attachment", `Are you sure you want to delete attachment: ${file.name}?`, ["Cancel", "Delete"])
            .done((result) => {
                if (result.can) {
                    new deleteAttachmentCommand(file.documentId, file.name, this.db())
                        .execute()
                        .done(() => this.loadDocument(file.documentId));
                }
            });
    }

    deleteCounter(counter: counterItem) {
        eventsCollector.default.reportEvent("counter", "delete");
        viewHelpers.confirmationMessage("Delete counter", `Are you sure you want to delete counter ${counter.counterName}?`, ["Cancel", "Delete"])
            .done((result) => {
                if (result.can) {
                    new deleteCounterCommand(counter.counterName, counter.documentId, this.db())
                        .execute()
                        .done(() => this.loadDocument(counter.documentId));
                }
            });
    }

    fetchAttachments(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>> {
        const doc = this.document();

        let attachments: documentAttachmentDto[] = doc.__metadata.attachments() || [];

        if (nameFilter) {
            attachments = attachments.filter(file => file.Name.toLocaleLowerCase().includes(nameFilter));
        }

        const mappedFiles = attachments.map(file => editDocument.mapToAttachmentItem(doc.getId(), file));

        return $.Deferred<pagedResult<attachmentItem>>().resolve({
            items: mappedFiles,
            totalResultCount: mappedFiles.length
        });
    }
    
    fetchCounters(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<counterItem>> {
        const doc = this.document();

        if (doc.__metadata.hasFlag("Revision")) {
            let counters = doc.__metadata.revisionCounters();

            if (nameFilter) {
                counters = counters.filter(c => c.name.toLocaleLowerCase().includes(nameFilter));
            }

            return $.when({
                items: counters.map(x => {
                    return {
                        documentId: doc.getId(),
                        counterName: x.name,
                        totalCounterValue: x.value,
                        counterValuesPerNode: []
                    } as counterItem;
                }),
                totalResultCount: counters.length
            });
        }

        if (!doc.__metadata.hasFlag("HasCounters")) {
            return connectedDocuments.emptyDocResult<counterItem>();
        }

        const fetchTask = $.Deferred<pagedResult<counterItem>>();
        new getCountersCommand(doc.getId(), this.db())
            .execute()
            .done(result => {
                if (nameFilter) {
                    result.Counters = result.Counters
                        .filter(x => x.CounterName.toLocaleLowerCase().includes(nameFilter));
                }
                const mappedResults = result.Counters
                    .map(x => normalCrudActions.resultItemToCounterItem(x));

                fetchTask.resolve({
                    items: mappedResults,
                    totalResultCount: result.Counters.length
                });

            })
            .fail(xhr => fetchTask.reject(xhr));

        return fetchTask.promise();
    }

    private static resultItemToCounterItem(counterDetail: Raven.Client.Documents.Operations.Counters.CounterDetail): counterItem {
        const counter = counterDetail;

        let valuesPerNode = Array<nodeCounterValue>();
        for (const nodeDetails in counter.CounterValues) {
            valuesPerNode.unshift({
                nodeTag: nodeDetails[0],
                nodeFullId: _.split(nodeDetails, ':')[1],
                nodeShortId: _.split(nodeDetails, '-')[0],
                nodeCounterValue: counter.CounterValues[nodeDetails]
            })
        }

        return {
            documentId: counter.DocumentId,
            counterName: counter.CounterName,
            totalCounterValue: counter.TotalValue,
            counterValuesPerNode: valuesPerNode
        };
    }
    
    saveRelatedItems(targetDocumentId: string) {
        // no action required
        return $.when<void>(null);
    }
    
    onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any): void | JQueryPromise<void> {
        this.onDocumentSavedAction(saveResult, localDoc);
    }
}

class clonedDocumentCrudActions implements editDocumentCrudActions {
    counters = ko.observableArray<counterItem>();
    attachments = ko.observableArray<attachmentItem>();
    
    attachmentsCount: KnockoutComputed<number>;
    countersCount: KnockoutComputed<number>;
    
    private readonly parentView: editDocument;
    private readonly sourceDocumentId: string;
    private readonly db: KnockoutObservable<database>;
    private readonly reload: () => void;
    
    private changeVector: string;
    private fromRevision: boolean;
    
    constructor(parentView: editDocument, db: KnockoutObservable<database>, attachments: attachmentItem[], counters: counterItem[], reload: () => void) {
        this.parentView = parentView;
        this.sourceDocumentId = parentView.editedDocId();
        
        this.db = db;
        this.reload = reload;

        const sourceDocument = parentView.document();
        this.fromRevision = sourceDocument.__metadata.hasFlag("Revision");
        this.changeVector = sourceDocument.__metadata.changeVector();
        
        this.attachments(attachments);
        this.counters(counters);

        _.bindAll(this, "setCounter");
        
        this.attachmentsCount = ko.pureComputed(() => this.attachments().length);
        this.countersCount = ko.pureComputed(() => this.counters().length);
    }
    
    setCounter(counter: counterItem) {
        const saveAction = (newCounter: boolean, counterName: string, newValue: number,
                            db: database, onCounterNameError: (error: string) => void): JQueryPromise<CountersDetail> => {

            const task = $.Deferred<CountersDetail>();
            
            const existing = this.counters().find(x => x.counterName === counterName);
            
            if (newCounter) {
                if (existing) {
                    const error = "Counter '" + counterName + "' already exists.";
                    onCounterNameError(error);
                    task.reject(error);
                } else {
                    this.counters.push({
                        counterName: counterName,
                        totalCounterValue: newValue,
                        documentId: null,
                        counterValuesPerNode: []
                    })
                }
            } else {
                existing.totalCounterValue = newValue;
            }
            
            this.reload();
            return task.resolve(null);
        };
        
        eventsCollector.default.reportEvent("counters", "set");
        const setCounterView = new setCounterDialog(counter, this.db(), false, saveAction);

        app.showBootstrapDialog(setCounterView);
    }

    deleteAttachment(file: attachmentItem) {
        this.attachments.remove(file);
        this.reload();
    }

    deleteCounter(counter: counterItem) {
        this.counters.remove(counter);
        this.reload();
    }

    fetchAttachments(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>> {
        let attachments: attachmentItem[] = this.attachments();

        if (nameFilter) {
            attachments = attachments.filter(file => file.name.toLocaleLowerCase().includes(nameFilter));
        }

        return $.Deferred<pagedResult<attachmentItem>>().resolve({
            items: attachments,
            totalResultCount: attachments.length
        });
    }

    fetchCounters(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<counterItem>> {
        let counters: counterItem[] = this.counters();

        if (nameFilter) {
            counters = counters.filter(counter => counter.counterName.toLocaleLowerCase().includes(nameFilter));
        }

        return $.Deferred<pagedResult<counterItem>>().resolve({
            items: counters,
            totalResultCount: counters.length
        });
    }
    
    saveRelatedItems(targetDocumentId: string): JQueryPromise<void> {
        const hasAttachments = this.attachmentsCount() > 0;
        const hasCounters = this.countersCount() > 0;
        if (hasAttachments || hasCounters) {
            
            const attachmentNames = this.attachments().map(x => x.name);
            const counters: Array<{ name: string, value: number }> = this.counters().map(x => {
                return {
                    name: x.counterName,
                    value: x.totalCounterValue
                }
            });
            
            return new cloneAttachmentsAndCountersCommand(this.sourceDocumentId, this.fromRevision, this.changeVector, 
                targetDocumentId, this.db(), attachmentNames, counters)
                .execute();
        } else {
            // no need for extra call
            return $.when<void>(null);
        }
    }
    
    onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any) {
        this.parentView.dirtyFlag().reset();
        router.navigate(appUrl.forEditDoc(saveResult.Results[0]["@id"], this.db()));
    }
}

export = editDocument;
