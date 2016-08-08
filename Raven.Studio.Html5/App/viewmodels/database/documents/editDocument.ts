/// <reference path="../../../../Scripts/typings/ace/ace.amd.d.ts" />

import app = require("durandal/app");
import router = require("plugins/router");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import documentMetadata = require("models/database/documents/documentMetadata");
import collection = require("models/database/documents/collection");
import querySort = require("models/database/query/querySort");

import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import resolveMergeCommand = require("commands/database/studio/resolveMergeCommand");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import genUtils = require("common/generalUtils");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import viewModelBase = require("viewmodels/viewModelBase");
import generateClassCommand = require("commands/database/documents/generateClassCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");

class editDocument extends viewModelBase {

    isConflictDocument = ko.observable<boolean>();
    document = ko.observable<document>();
    metadata: KnockoutComputed<documentMetadata>;
    documentText = ko.observable("").extend({ required: true });
    metadataText = ko.observable("").extend({ required: true });
    text: KnockoutComputed<string>;
    isEditingMetadata = ko.observable(false);
    isBusy = ko.observable(false);
    metaPropsToRestoreOnSave = [];
    editedDocId: KnockoutComputed<string>;
    userSpecifiedId = ko.observable("").extend({ required: true });
    isCreatingNewDocument = ko.observable(false);
    docsList = ko.observable<pagedList>();
    queryResultList = ko.observable<pagedList>();
    currentQueriedItemIndex:number;
    docEditor: AceAjax.Editor;
    documentNameElement: JQuery;
    databaseForEditedDoc: database;
    topRecentDocuments = ko.computed(() => this.getTopRecentDocuments());
    relatedDocumentHrefs = ko.observableArray<{id:string;href:string}>();
    docEditroHasFocus = ko.observable(true);
    documentMatchRegexp = /\w+\/\w+/ig;
    loadedDocumentName = ko.observable<string>("");
    isSaveEnabled: KnockoutComputed<boolean>;
    documentSize: KnockoutComputed<string>;
    isInDocMode = ko.observable(true);
    queryIndex = ko.observable<String>();
    docTitle: KnockoutComputed<string>;
    isNewLineFriendlyMode = ko.observable(false);
    autoCollapseMode = ko.observable(false);
    isFirstDocumenNavtDisabled: KnockoutComputed<boolean>;
    isLastDocumentNavDisabled: KnockoutComputed<boolean>;
    newLineToggle = '\\n';
    isSystemDocumentByDocTitle = ko.observable(false);
    isSaving = ko.observable<boolean>(false);
    documentChangeNotificationMessage = ko.observable<string>("");
    changeNotification: changeSubscription;
    isDeleting: boolean = false;

    static editDocSelector = "#editDocumentContainer";
    static recentDocumentsInDatabases = ko.observableArray<{ databaseName: string; recentDocuments: KnockoutObservableArray<string> }>();

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.metadata = ko.computed(() => this.document() ? this.document().__metadata : null);
        this.isConflictDocument = ko.computed(() => {
            var metadata = this.metadata();

            return metadata != null && !!metadata["Raven-Replication-Conflict"] && !metadata.id.contains("/conflicts/");
        });

        this.document.subscribe(doc => {
            if (doc) {
                if (this.isConflictDocument()) {
                    this.resolveConflicts();
                } else {
                    var docText = this.stringify(doc.toDto());
                    this.documentText(docText);
                }
            }
        });

        this.documentSize = ko.computed(() => {
            try {
                var size: Number = ((this.documentText().getSizeInBytesAsUTF8() + this.metadataText().getSizeInBytesAsUTF8()) / 1024);
                return genUtils.formatAsCommaSeperatedString(size,2);    
            } catch (e) {
                return "cannot compute";
            } 
            
        });

        this.metadata.subscribe((meta: documentMetadata) => this.metadataChanged(meta));
        this.editedDocId = ko.computed(() => this.metadata() ? this.metadata().id : '');
        this.editedDocId.subscribe((docId: string) =>
            ko.postbox.publish("SetRawJSONUrl", docId ? appUrl.forDocumentRawData(this.activeDatabase(), docId) : "")
        );

        // When we programmatically change the document text or meta text, push it into the editor.
        this.isEditingMetadata.subscribe(()=> {
            if (this.docEditor) {
                var text = this.isEditingMetadata() ? this.metadataText() : this.documentText();
                this.docEditor.getSession().setValue(text);
            }
        });
        this.text = ko.computed({
            read: () => {
                return this.isEditingMetadata() ? this.metadataText() : this.documentText();
            },
            write: (text: string) => {
                var currentObservable = this.isEditingMetadata() ? this.metadataText : this.documentText;
                currentObservable(text);
            },
            owner: this
        });


        this.docTitle = ko.computed(() => {
            if (this.isInDocMode()) {
                if (this.isCreatingNewDocument()) {
                    this.isSystemDocumentByDocTitle(false);
                    return "New Document";
                } else {
                    var editedDocId = this.editedDocId();

                    if (!!editedDocId) {
                        if (editedDocId.indexOf("Raven/") === 0) {
                            this.isSystemDocumentByDocTitle(true);
                        } else {
                            this.isSystemDocumentByDocTitle(false);
                        }
                            
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

        this.isFirstDocumenNavtDisabled = ko.computed(() => {
            var list = this.docsList();
            if (list) {
                var currentDocumentIndex = list.currentItemIndex();

                if (currentDocumentIndex === 0) {
                    return true;
                }
            }

            return false;
        });

        this.isLastDocumentNavDisabled = ko.computed(() => {
            var list = this.docsList();
            if (list) {
                var currentDocumentIndex = list.currentItemIndex();
                var totalDocuments = list.totalResultCount();

                if (currentDocumentIndex === totalDocuments - 1) {
                    return true;
                }
            }

            return false;
        });
    }

    canActivate(args: any) {
        super.canActivate(args);
        var canActivateResult = $.Deferred();
        if (args && args.id) {
            
            this.databaseForEditedDoc = appUrl.getDatabase();
            this.loadDocument(args.id)
                .done(() => {
                    this.changeNotification = this.createDocumentChangeNotification(args.id);
                    this.addNotification(this.changeNotification);
                    canActivateResult.resolve({ can: true });
                })
                .fail(() => {
                    messagePublisher.reportError("Could not find " + args.id + " document");
                    canActivateResult.resolve({ redirect: appUrl.forDocuments(collection.allDocsCollectionName, this.activeDatabase()) });
                });
            return canActivateResult;
        } else if (args && args.item && args.list) {
            return $.Deferred().resolve({ can: true }); //todo: maybe treat case when there is collection and item number but no id
        }
        else if (args && args.index) {
            this.isInDocMode(false);
            var indexName: string = args.index;
            var queryText: string = args.query;
            var sorts: querySort[];
            
            if (args.sorts) {
                sorts = args.sorts.split(',').map((curSort: string) => querySort.fromQuerySortString(curSort.trim()));
                
        } else {
                sorts = [];
            }
                
            var resultsFetcher = (skip: number, take: number) => {
                var command = new queryIndexCommand(indexName, this.activeDatabase(), skip, take, queryText, sorts);
                return command
                    .execute();
            };
            var list = new pagedList(resultsFetcher);
            var item = !!args.item && !isNaN(args.item) ? args.item : 0;
            
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
            this.currentQueriedItemIndex = item;
            this.queryResultList(list);
            this.queryIndex(indexName);
            return canActivateResult;
        }
        else{
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);
        this.updateHelpLink('M72H1R');

        this.loadedDocumentName(this.userSpecifiedId());
        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.metadataText, this.userSpecifiedId],false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.computed(()=> {
            return (this.isSaving() === false && (this.dirtyFlag().isDirty() || this.loadedDocumentName() == ""));// && !!self.userSpecifiedId(); || 
        }, this);

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
            this.appendRecentDocument(navigationArgs.id);

            ko.postbox.publish("SetRawJSONUrl", appUrl.forDocumentRawData(this.activeDatabase(), navigationArgs.id));
        } else if (navigationArgs && navigationArgs.index) {
            //todo: implement SetRawJSONUrl for document from query
        }
        else{
            this.editNewDocument();
        }
    }

    // Called when the view is attached to the DOM.
    attached() {
        super.attached();
        this.setupKeyboardShortcuts();
        $("#docEditor").resize();
        this.isNewLineFriendlyMode.subscribe(val => {
            this.updateNewlineLayoutInDocument(val);
        });
    }

    detached() {
        super.detached();
        $("#docEditor").off('DynamicHeightSet');
    }

    compositionComplete() {
        super.compositionComplete();

        this.documentNameElement = $("#documentName");

        var editorElement = $("#docEditor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#docEditor").on('DynamicHeightSet', () => this.docEditor.resize());
        $("#docEditor").bind("paste", function () { this.text.valueHasMutated() });
        this.focusOnEditor();
    }

    createDocumentChangeNotification(docId: string) {
        return changesContext.currentResourceChangesApi().watchDocument(docId, (n: documentChangeNotificationDto) => this.documentChangeNotification(n));
    }

    documentChangeNotification(n: documentChangeNotificationDto) {
        if (this.isSaving() || this.isDeleting) {
            return;
        }

        var newEtag = n.Etag;
        if (newEtag === this.metadata().etag) {
            return;
        }

        var message;
        if (newEtag == null) {
            message = "Document was deleted";
        } else {
            message = "Document was changed, new Etag: " + n.Etag;
        }

        this.documentChangeNotificationMessage(message);
        $(".changeNotification").highlight();
    }

    updateNewlineLayoutInDocument(unescapeNewline) {
        var dirtyFlagValue = this.dirtyFlag().isDirty();
        if (unescapeNewline == true) {
            this.documentText(this.unescapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/json_newline_friendly');
        } else {
            this.documentText(this.escapeNewlinesAndTabsInTextFields(this.documentText()));
            this.docEditor.getSession().setMode('ace/mode/json');
            this.formatDocument();
        }

        if (dirtyFlagValue == false) {
            this.dirtyFlag().reset();
        }
    }

    setupKeyboardShortcuts() {       
        this.createKeyboardShortcut("alt+shift+d", () => this.focusOnDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+m", () => this.focusOnMetadata(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+r", () => this.refreshDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+home", () => this.firstDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+end", () => this.lastDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+page-up", () => this.previousDocumentOrLast(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+page-down", () => this.nextDocumentOrFirst(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteDocument(), editDocument.editDocSelector);
        this.createKeyboardShortcut("alt+s", () => this.saveDocument(), editDocument.editDocSelector); // Q. Why do we have to setup ALT+S, when we could just use HTML's accesskey attribute? A. Because the accesskey attribute causes the save button to take focus, thus stealing the focus from the user's editing spot in the doc editor, disrupting his workflow.
        //this.createKeyboardShortcut("/", () => this.docsList(), editDocument.editDocSelector);
    }

    focusOnMetadata() {
        this.isEditingMetadata(true);
        this.focusOnEditor();
    }

    focusOnDocument() {
        this.isEditingMetadata(false);
        this.focusOnEditor();
    }

    private focusOnEditor() {
        this.docEditor.focus();
    }

    editNewDocument() {
        this.isCreatingNewDocument(true);
        var newDocument = document.empty();
        newDocument["Name"] = "...";
        this.document(newDocument);
    }

    failedToLoadDoc(docId, errorResponse) {
        messagePublisher.reportError("Could not find " + docId + " document");
    }

    escapeNewlinesAndTabsInTextFields(str: string) :any {
        var AceDocumentClass = require("ace/document").Document;
        var AceEditSessionClass = require("ace/edit_session").EditSession;
        var AceJSONMode = require("ace/mode/json_newline_friendly").Mode;
        var documentTextAceDocument = new AceDocumentClass(str);
        var jsonMode = new AceJSONMode();
        var documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        var previousLine = 0;

        var TokenIterator = require("ace/token_iterator").TokenIterator;
        var iterator = new TokenIterator(documentTextAceEditSession, 0, 0);
        var curToken = iterator.getCurrentToken();
        var text = "";
        while (curToken) {
            if (iterator.$row - previousLine > 1) {
                var rowsGap = iterator.$row - previousLine;
                for (var i = 0; i < rowsGap -1; i++) {
                    text += "\\r\\n";
                }
            }
            if (curToken.type === "string" || curToken.type == "constant.language.escape") {
                if (previousLine < iterator.$row) {
                    text += "\\r\\n";
                }

                var newTokenValue = curToken.value
                    //.replace(/(\\n|\\r\\n)/g, '\\\\r\\\\n')
                    //.replace(/(\n|\r\n)/g, '\\r\\n')
                    //.replace(/(\\t)/g, '\\\\t')
                    //.replace(/(\t)/g, '\\t');                    
                    .replace(/(\r\n)/g, '\\r\\n')
                    .replace(/(\n)/g, '\\n')
                    .replace(/(\t)/g, '\\t');
                text += newTokenValue;
                //text += curToken.value.replace(/(\n|\r\n)/g, '\\r\\n');
            } else {
                text += curToken.value;
            }

            previousLine = iterator.$row;
            curToken = iterator.stepForward();
        }

        return text;
    }

    toggleNewlineMode() {
        if (this.isNewLineFriendlyMode() === false && parseInt(this.documentSize().replace(",", "")) > 150) {
            app.showMessage("This operation might take long time with big documents, are you sure you want to continue?", "Toggle newline mode", ["Cancel", "Continue"])
                .then((dialogResult: string) => {
                    if (dialogResult === "Continue") {
                        this.isNewLineFriendlyMode.toggle();
                    }
                });
        }
        else
        {
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

    unescapeNewlinesAndTabsInTextFields(str: string): any {
        var AceDocumentClass = require("ace/document").Document;
        var AceEditSessionClass = require("ace/edit_session").EditSession;
        var AceJSONMode = require("ace/mode/json").Mode;
        var documentTextAceDocument = new AceDocumentClass(str);
        var jsonMode = new AceJSONMode();
        var documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        var TokenIterator = require("ace/token_iterator").TokenIterator;
        var iterator = new TokenIterator(documentTextAceEditSession, 0, 0);
        var curToken = iterator.getCurrentToken();
        // first, calculate newline indexes
        var rowsIndexes = str.split("").map(function (x, index) {return { char: x, index: index } }).filter(function (x) {return x.char == "\n" }).map(function (x) {return x.index });

        

        // start iteration from the end of the document
        while (curToken) {
            curToken = iterator.stepForward();
        }
        curToken = iterator.stepBackward();

        var lastTextSectionPosEnd = null;
        
        while (curToken) {
            if (curToken.type === "string" || curToken.type == "constant.language.escape") {
                if (lastTextSectionPosEnd == null) {
                    curToken = iterator.stepForward();
                    lastTextSectionPosEnd = { row: iterator.getCurrentTokenRow(), column: iterator.getCurrentTokenColumn() + 1 };
                    curToken = iterator.stepBackward();
                }
            }
            else {
                if (lastTextSectionPosEnd != null) {
                    curToken = iterator.stepForward();
                    var lastTextSectionPosStart = { row: iterator.getCurrentTokenRow(), column: iterator.getCurrentTokenColumn() + 1 };
                    var stringTokenStartIndexInSourceText = (lastTextSectionPosStart.row > 0 ?  rowsIndexes[lastTextSectionPosStart.row-1]:0) + lastTextSectionPosStart.column;
                    var stringTokenEndIndexInSourceText = (lastTextSectionPosEnd.row > 0 ?rowsIndexes[lastTextSectionPosEnd.row-1]:0) + lastTextSectionPosEnd.column;
                    var newTextPrefix = str.substring(0, stringTokenStartIndexInSourceText);
                    var newTextSuffix = str.substring(stringTokenEndIndexInSourceText, str.length);
                    var newStringTokenValue = str.substring(stringTokenStartIndexInSourceText, stringTokenEndIndexInSourceText)
                        .replace(/(\\\\n|\\\\r\\\\n|\\n|\\r\\n|\\t|\\\\t)/g, (x) => {
                        if (x == "\\\\n" || x == "\\\\r\\\\n") {
                            return "\\r\\n";
                        } else if (x == "\\n" || x == "\\r\\n") {
                            return "\r\n";
                        } else if (x == "\\t") {
                            return "\t";
                        } else if (x == "\\\\t") {
                            return "\\t";
                        } else {
                            return "\r\n";
                        }
                        });

                    str = newTextPrefix + newStringTokenValue + newTextSuffix ;
                    curToken = iterator.stepBackward();
                }
                lastTextSectionPosEnd = null;
            }
            
            curToken = iterator.stepBackward();
        }

        return str;
    }

    saveDocument() {
        this.isSaving(true);
        this.isInDocMode(true);
        var currentDocumentId = this.userSpecifiedId();
        var loadedDocumentName = this.loadedDocumentName();
        if ((currentDocumentId == "") || (loadedDocumentName != currentDocumentId)) {
            //the name of the document was changed and we have to save it as a new one
            this.isCreatingNewDocument(true);
        }

        if (currentDocumentId.indexOf("\\") !== -1) {
            this.documentNameElement.focus();
            messagePublisher.reportError("Document name cannot contain '\\'", undefined, undefined, false);
            this.isSaving(false);
            return;
        }

        var updatedDto;
        var meta;
        try {
            if (this.isNewLineFriendlyMode()) {
                updatedDto = JSON.parse(this.escapeNewlinesAndTabsInTextFields(this.documentText()));
            } else {
                updatedDto = JSON.parse(this.documentText());
            }
            meta = JSON.parse(this.metadataText());
        } catch (e) {
            var message = e;
            if (updatedDto == undefined) {
                message = "The document data isn't a legal JSON expression!";
                this.isEditingMetadata(false);
            } else if (meta == undefined) {
                message = "The document metadata isn't a legal JSON expression!";
                this.isEditingMetadata(true);
            }

            this.focusOnEditor();
            messagePublisher.reportError(message, undefined, undefined, false);
            this.isSaving(false);
            return;
        }
        
        updatedDto["@metadata"] = meta;

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
            if (metaToSkipInHeaders.hasOwnProperty(i) == false)
                continue;

            var skippedHeader = metaToSkipInHeaders[i];
            delete meta[skippedHeader];
        }

        if (!!this.docsList()) {
            this.docsList().invalidateCache();
        }

        var newDoc = new document(updatedDto);
        var saveCommand = new saveDocumentCommand(currentDocumentId, newDoc, this.activeDatabase());
        saveCommand.execute()
            .done((saveResult: bulkDocumentDto[]) => {
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

                        this.changeNotification = this.createDocumentChangeNotification(savedDocumentDto.Key);
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
                var list: pagedList = this.docsList();
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
            })
            .fail(() => this.isSaving(false));
    }

    attachReservedMetaProperties(id: string, target: documentMetadataDto) {
        target['@etag'] = '00000000-0000-0000-0000-000000000000';
        target['Raven-Entity-Name'] = !target['Raven-Entity-Name'] ? document.getEntityNameFromId(id) : target['Raven-Entity-Name'];
        target['@id'] = id;
    }

    stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    activateMeta() {
        this.isEditingMetadata(true);
        this.docEditor.getSession().setMode('ace/mode/json');
    }

    activateDoc() {
        this.isEditingMetadata(false);

        if (this.isNewLineFriendlyMode() == true) {
            this.docEditor.getSession().setMode('ace/mode/json_newline_friendly');
        }
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
        loadDocTask.done((document: document)=> {
            this.document(document);
            this.loadedDocumentName(this.userSpecifiedId());
            this.dirtyFlag().reset(); //Resync Changes

            this.loadRelatedDocumentsList(document);
            this.appendRecentDocument(id);
            if (this.autoCollapseMode()) {
                this.foldAll();
            }
        });
        loadDocTask.fail(response => this.failedToLoadDoc(id, response));
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    refreshDocument() {
        var canContinue = this.canContinueIfNotDirty("Refresh", "You have unsaved data. Are you sure you want to continue?");
        canContinue.done(() => {
            if (this.isInDocMode()) {
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
            } else {
                this.queryResultList().getNthItem(this.currentQueriedItemIndex).done((doc) => this.document(doc));
                this.loadedDocumentName("");
            }

            this.documentChangeNotificationMessage("");
        });
    }

    deleteDocument() {
        var doc: document = this.document();
        if (doc) {
            this.isDeleting = true;

            var viewModel = new deleteDocuments([doc]);
            viewModel.deletionTask
                .done(() => {
                    this.dirtyFlag().reset(); //Resync Changes

                    var list = this.docsList();
                    if (!list) {
                        router.navigate(appUrl.forDocuments(null, this.activeDatabase()));
                        return;
                    }

                    this.docsList().invalidateCache();

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
                })
                .always(() => this.isDeleting = false);

            app.showDialog(viewModel, editDocument.editDocSelector);
        } 
    }

    formatDocument() {
        try {
            var docEditorText = this.docEditor.getSession().getValue();
            var observableToUpdate = this.isEditingMetadata() ? this.metadataText : this.documentText;
            var tempDoc = JSON.parse(docEditorText);
            var formatted = this.stringify(tempDoc);
            observableToUpdate(formatted);
        } catch (e) {
            messagePublisher.reportError("Could not format json", undefined, undefined, false);
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

    pageToItem(index: number, newTotalResultCount?: number) {
        var canContinue = this.canContinueIfNotDirty("Unsaved Data", "You have unsaved data. Are you sure you want to continue?");
        canContinue.done(() => {
            var list = this.docsList();
            if (list) {
                list.getNthItem(index)
                    .done((doc: document) => {
                        if (this.isInDocMode()) {
                            var docId = doc.getId();
                            this.loadDocument(docId).done(() => {
                                this.changeNotification.off();
                                this.removeNotification(this.changeNotification);

                                this.changeNotification = this.createDocumentChangeNotification(docId);
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
        var collectionName = this.docsList() ? this.docsList().collectionName : null;
        var currentItemIndex = this.docsList() ? this.docsList().currentItemIndex() : null;
        var editDocUrl = appUrl.forEditDoc(docId, collectionName, currentItemIndex, this.activeDatabase());
        router.navigate(editDocUrl, false);
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
                        docId: (docId.length > 35) ? docId.substr(0,35) + '...' : docId,
                        docUrl: appUrl.forEditDoc(docId, null, null, this.activeDatabase()),
                        fullDocId : docId 
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

            documentMetadata.filterMetadata(metaDto, this.metaPropsToRestoreOnSave);

            var metaString = this.stringify(metaDto);
            this.metadataText(metaString);
            if (meta.id != undefined) {
                this.userSpecifiedId(meta.id);
            }
        }
    }

    loadRelatedDocumentsList(document) {
        var relatedDocumentsCandidates: string[] = this.findRelatedDocumentsCandidates(document);
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
    }

    appendRecentDocument(docId: string) {

        var existingRecentDocumentsStore = editDocument.recentDocumentsInDatabases.first(x=> x.databaseName == this.databaseForEditedDoc.name);
        if (existingRecentDocumentsStore) {
            var existingDocumentInStore = existingRecentDocumentsStore.recentDocuments.first(x=> x === docId);
            if (!existingDocumentInStore) {
                if (existingRecentDocumentsStore.recentDocuments().length == 5) {
                    existingRecentDocumentsStore.recentDocuments.pop();
                }
                existingRecentDocumentsStore.recentDocuments.unshift(docId);
            }

        } else {
            editDocument.recentDocumentsInDatabases.push({ databaseName: this.databaseForEditedDoc.name, recentDocuments: ko.observableArray([docId]) });
        }

    }

    resolveConflicts() {
        var task = new resolveMergeCommand(this.activeDatabase(), this.editedDocId()).execute();
        task.done((response: mergeResult) => {
            this.documentText(response.Document);
            this.metadataText(response.Metadata);
        });
    }

    getColorClass(documentId: string) {
        var entityName = document.getEntityNameFromId(documentId);
        if (entityName) {
            return collection.getCollectionCssClass(entityName, this.activeDatabase());
        }

        return "";
    }

    prettyLabel(text: string) {
        return text ? text.replace(/__/g, '/') : text;
    }

    generateCollectionName(ravenEntityName: string, withPrettyLabel: boolean = false) {
        if (withPrettyLabel) {
            return ravenEntityName
                ? this.prettyLabel(ravenEntityName)
                : (this.isSystemDocumentByDocTitle() ? 'System Documents' : 'No Collection');
        } else {
            return ravenEntityName || (this.isSystemDocumentByDocTitle() ? 'System Documents' : 'No Collection');    
        }
    }

    generateCode() {
        var doc: document = this.document();
        var generate = new generateClassCommand(this.activeDatabase(), doc.getId(), "csharp");
        var deffered = generate.execute();
        deffered.done((code: JSON) => {
            app.showDialog(new showDataDialog("Generated Class", code["Code"]));
        });
    }
}

export = editDocument;
