import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import patchCommand = require("commands/database/patch/patchCommand");
import eventsCollector = require("common/eventsCollector");
import notificationCenter = require("common/notifications/notificationCenter");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import documentPropertyProvider = require("common/helpers/database/documentPropertyProvider");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import defaultAceCompleter = require("common/defaultAceCompleter");
import patchSyntax = require("viewmodels/database/patch/patchSyntax");
import patchTester = require("viewmodels/database/patch/patchTester");
import savedPatchesStorage = require("common/storage/savedPatchesStorage");
import queryUtil = require("common/queryUtil");
import generalUtils = require("common/generalUtils");
import queryCommand = require("commands/database/query/queryCommand");
import queryCriteria = require("models/database/query/queryCriteria");
import rqlLanguageService = require("common/rqlLanguageService");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

type fetcherType = (skip: number, take: number, previewCols: string[], fullCols: string[]) => JQueryPromise<pagedResult<document>>;

class patchList {

    private readonly recentPatchLimit = 6;

    previewItem = ko.observable<storedPatchDto>();

    allPatches = ko.observableArray<storedPatchDto>([]);

    private readonly useHandler: (patch: storedPatchDto) => void;
    private readonly removeHandler: (patch: storedPatchDto) => void;

    hasAnySavedPatch = ko.pureComputed(() => this.allPatches().length > 0);

    previewCode = ko.pureComputed(() => {
        const item = this.previewItem();
        if (!item) {
            return "";
        }

        return item.Query;
    });

    constructor(useHandler: (patch: storedPatchDto) => void, removeHandler: (patch: storedPatchDto) => void) {
        _.bindAll(this, ...["previewPatch", "removePatch", "usePatch", "usePatchItem"] as Array<keyof this & string>);
        this.useHandler = useHandler;
        this.removeHandler = removeHandler;
    }

    filteredPatches = ko.pureComputed(() => {
        let text = this.filters.searchText();

        if (!text) {
            return this.allPatches();
        }

        text = text.toLowerCase();

        return this.allPatches().filter(x => x.Name.toLowerCase().includes(text));
    });

    filters = {
        searchText: ko.observable<string>()
    };

    previewPatch(item: storedPatchDto) {
        this.previewItem(item);
    }

    usePatch() {
        this.useHandler(this.previewItem());
    }

    usePatchItem(item: storedPatchDto) {
        this.previewItem(item);
        this.usePatch();
    } 

    removePatch(item: storedPatchDto) {
        if (this.previewItem() === item) {
            this.previewItem(null);
        }
        this.removeHandler(item);
    }

    loadAll(db: database) {
        this.allPatches(savedPatchesStorage.getSavedPatches(db));
    }     

    append(doc: storedPatchDto) {
        if (doc.RecentPatch) {
            const existing = this.allPatches().find(patch => patch.Hash === doc.Hash);
            if (existing) {
                this.allPatches.remove(existing);
                this.allPatches.unshift(doc);
            } else {
                this.removeLastRecentPatchIfMoreThanLimit();
                this.allPatches.unshift(doc);
            }
        } else {
            const existing = this.allPatches().find(x => x.Name === doc.Name);
            if (existing) {
                this.allPatches.replace(existing, doc);
            } else {
                this.allPatches.unshift(doc);
            }
        }
    }

    private removeLastRecentPatchIfMoreThanLimit() {
        this.allPatches()
            .filter(x => x.RecentPatch)
            .filter((_, idx) => idx >= this.recentPatchLimit)
            .forEach(x => this.allPatches.remove(x));
    }
}

class patch extends viewModelBase {
    
    view = require("views/database/patch/patch.html");

    staleIndexBehavior = ko.observable("patchStale"); 
    staleTimeout = ko.observable<number>(60);

    maxOperationsPerSecond = ko.observable<number>();
    defineMaxOperationsPerSecond = ko.observable<boolean>(false);
    
    disableAutoIndexCreation = ko.observable<boolean>(false);
    
    static readonly recentKeyword = 'Recent Patch';

    static readonly $body = $("body");
    static readonly ContainerSelector = "#patchContainer";
    static readonly patchSaveSelector = ".patch-save";

    static lastQuery = new Map<string, string>();

    patchSaveName = ko.observable<string>();
    patchSaveFocus = ko.observable<boolean>(false);
    saveValidationGroup: KnockoutValidationGroup;

    inSaveMode = ko.observable<boolean>();

    spinners = {
        save: ko.observable<boolean>(false),
        countMatchingDocuments: ko.observable<boolean>(false)
    };

    jsCompleter = defaultAceCompleter.completer();
    private indexes = ko.observableArray<Raven.Client.Documents.Operations.IndexInformation>();
    languageService: rqlLanguageService;
    
    private fullDocumentsProvider: documentPropertyProvider;

    patchDocument = ko.observable<patchDocument>(patchDocument.empty());

    savedPatches = new patchList(item => this.usePatch(item), item => this.removePatch(item));

    test = new patchTester(this.patchDocument().query, this.activeDatabase);

    private hideSavePatchHandler = (e: Event) => {
        if ($(e.target).closest(".patch-save").length === 0) {
            this.inSaveMode(false);
        }
    };

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.languageService = new rqlLanguageService(this.activeDatabase, this.indexes, "Update");

        this.bindToCurrentInstance("savePatch");
        this.initObservables();
    }

    private initValidation() {
        this.saveValidationGroup = ko.validatedObservable({
            patchSaveName: this.patchSaveName
        });
    }

    private initObservables() {
        this.inSaveMode.subscribe(enabled => {
            const $input = $(".patch-save .form-control");
            if (enabled) {
                $input.show();
                window.addEventListener("click", this.hideSavePatchHandler, true);
            } else {
                this.saveValidationGroup.errors.showAllMessages(false);
                window.removeEventListener("click", this.hideSavePatchHandler, true);
                setTimeout(() => $input.hide(), 200);
            }
        });
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");

        this.fullDocumentsProvider = new documentPropertyProvider(this.activeDatabase());

        this.loadLastQuery();

        this.disableAutoIndexCreation(activeDatabaseTracker.default.settings().disableAutoIndexCreation.getValue());
        
        return $.when<any>(this.fetchAllIndexes(this.activeDatabase()), this.savedPatches.loadAll(this.activeDatabase()));
    }

    private loadLastQuery() {
        const myLastQuery = patch.lastQuery.get(this.activeDatabase().name);

        if (myLastQuery) {
            this.patchDocument().query(myLastQuery);
        }
    }

    deactivate(): void {
        super.deactivate();

        const queryText = this.patchDocument().query();
        this.saveLastQuery(queryText);
    }

    private saveLastQuery(queryText: string) {
        patch.lastQuery.set(this.activeDatabase().name, queryText);
    }

    attached() {
        super.attached();

        this.initValidation();

        this.createKeyboardShortcut("ctrl+enter", () => {
            if (this.test.testMode()) {
                this.test.runTest();
            } else {
                this.runPatch();
            }
        }, patch.ContainerSelector);
        
        this.createKeyboardShortcut("ctrl+s", () => {
            if (!this.inSaveMode()) {
                this.savePatch();
                this.patchSaveFocus(true);
            }
        }, patch.ContainerSelector);
        
        this.createKeyboardShortcut("enter", () => {
            this.savePatch();
        }, patch.patchSaveSelector);
    }

    private showPreview(doc: document) {
        // if document doesn't have all properties fetch them and then display preview

        const meta = doc.__metadata as any;
        const hasCollapsedFields = meta[getDocumentsPreviewCommand.ObjectStubsKey] || meta[getDocumentsPreviewCommand.ArrayStubsKey] || meta[getDocumentsPreviewCommand.TrimmedValueKey];

        if (hasCollapsedFields) {
            new getDocumentWithMetadataCommand(doc.getId(), this.activeDatabase(), true)
                .execute()
                .done((fullDocument: document) => {
                    documentBasedColumnsProvider.showPreview(fullDocument);
                });
        } else {
            // document has all properties - fallback to default method
            documentBasedColumnsProvider.showPreview(doc);
        }
    }
    
    compositionComplete() {
        super.compositionComplete();

        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));

        this.patchDocument().query.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }

    usePatch(item: storedPatchDto) {
        this.patchDocument().copyFrom(item);
    }

    removePatch(item: storedPatchDto) {

        this.confirmationMessage("Patch", `Are you sure you want to delete patch '${generalUtils.escapeHtml(item.Name)}'?`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    savedPatchesStorage.removeSavedPatchByHash(this.activeDatabase(), item.Hash);
                    this.savedPatches.loadAll(this.activeDatabase());
                }
            });
    }

    runPatch() {
        if (this.isValid(this.patchDocument().validationGroup)) {
            this.spinners.countMatchingDocuments(true);
            
            this.getMatchingDocumentsNumber()
                .done((matchingDocs: number) => {
                    this.spinners.countMatchingDocuments(false);
                    this.executePatch(matchingDocs);
                });
        }
    }

    savePatch() {
        if (this.inSaveMode()) {
            eventsCollector.default.reportEvent("patch", "save");

            if (this.isValid(this.saveValidationGroup) && this.isValid(this.patchDocument().validationGroup)) {

                // Verify if name already exists
                if (_.find(savedPatchesStorage.getSavedPatches(this.activeDatabase()), x => x.Name.toUpperCase() === this.patchSaveName().toUpperCase())) { 
                    this.confirmationMessage(`Patch ${generalUtils.escapeHtml(this.patchSaveName())} already exists`, `Overwrite existing patch ?`, {
                        buttons: ["No", "Overwrite"],
                        html: true
                    })
                        .done(result => {
                            if (result.can) {
                                this.savePatchToStorage();
                            }
                        });
                } else {
                    this.savePatchToStorage();
                }
                
                this.inSaveMode(false);
            }
        } else {
            if (this.isValid(this.patchDocument().validationGroup)) {
                this.inSaveMode(true);
            }
        }
    }
    
    detached() {
        super.detached();
        
        this.languageService.dispose();
        // clean up virtual view - unbind subscriptions
        this.test.detached();
    }

    private savePatchToStorage() {
        this.patchDocument().name(this.patchSaveName());
        this.savePatchInStorage(false);
        this.patchSaveName(null);
        this.saveValidationGroup.errors.showAllMessages(false);
        messagePublisher.reportSuccess("Patch saved successfully");
    }
    
    private saveRecentPatch() {
        const name = this.getRecentPatchName();
        this.patchDocument().name(name);
        this.savePatchInStorage(true);
    }

    private savePatchInStorage(isRecent: boolean) {
        const dto = this.patchDocument().toDto();
        dto.RecentPatch = isRecent;
        this.savedPatches.append(dto);
        savedPatchesStorage.storeSavedPatches(this.activeDatabase(), this.savedPatches.allPatches());

        this.patchDocument().name("");
        this.savedPatches.loadAll(this.activeDatabase());
    }

    showFirstItemInPreviewArea() {
        this.savedPatches.previewItem(savedPatchesStorage.getSavedPatches(this.activeDatabase())[0]);
    }
    
    private getRecentPatchName(): string {
        const [collectionIndexName, type] = queryUtil.getCollectionOrIndexName(this.patchDocument().query());
        return type !== "unknown" ? patch.recentKeyword + " (" + collectionIndexName + ")" : patch.recentKeyword;
    }

    private getMatchingDocumentsNumber(): JQueryPromise<number> {
        const patchScript = this.patchDocument().query();
        const patchScriptParts = patchScript.split("update");
        
        const matchingDocs = $.Deferred<number>();
        
        if (patchScriptParts.length === 2) {
            let query = queryCriteria.empty();
            query.queryText(patchScriptParts[0]);

            new queryCommand(this.activeDatabase(), 0, 0, query)
                .execute()
                .done((queryResults: pagedResultExtended<document>) => matchingDocs.resolve(queryResults.totalResultCount))
                .fail(() => matchingDocs.resolve(-1))
        } else {
            matchingDocs.resolve(-1);
        }

        return matchingDocs;
    }
    
    private executePatch(matchingDocuments: number): void {
        eventsCollector.default.reportEvent("patch", "run");

        const patchQuestion = `<div>Are you sure you want to apply this patch to matching documents?</div>`;
        
        const warningMessage = `<li>
                                 <small>Actual number of processed documents might be smaller if documents are filtered by the 'update' script</small>
                             </li>`;
        
        const patchMessage = matchingDocuments > -1 ?
                             `<div class="margin-bottom margin-bottom-lg text-info bg-info padding padding-xs">
                                 <ul class="margin-top">
                                     <li>
                                         <small>Number of documents matching the Patch Query: <strong class="margin-left margin-left-sm">${matchingDocuments.toLocaleString()}</strong></small>
                                     </li>
                                     ${matchingDocuments > 0 ? warningMessage : ''}
                                 </ul>
                              </div>
                              ${patchQuestion}` : `${patchQuestion}`;

        this.confirmationMessage("Patch", patchMessage, {
            buttons: ["Cancel", "Patch all"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    new patchCommand(this.patchDocument().query(), this.activeDatabase(), {
                        allowStale: this.staleIndexBehavior() === "patchStale",
                        staleTimeout: this.staleIndexBehavior() === "timeoutDefined" ? generalUtils.formatAsTimeSpan(this.staleTimeout() * 1000) : undefined,
                        maxOpsPerSecond: this.maxOperationsPerSecond(),
                        disableAutoIndexCreation: this.disableAutoIndexCreation()
                    })
                        .execute()
                        .done((operation: operationIdDto) => {
                            notificationCenter.instance.openDetailsForOperationById(this.activeDatabase(), operation.OperationId);
                            this.saveLastQuery("");
                            this.saveRecentPatch();
                        });
                }
            });
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.indexes(results.Indexes);
            });
    }

    syntaxHelp() {
        const viewModel = new patchSyntax();
        app.showBootstrapDialog(viewModel);
    }
}

export = patch;
