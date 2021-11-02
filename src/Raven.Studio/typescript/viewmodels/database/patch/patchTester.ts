import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import viewHelpers = require("common/helpers/view/viewHelpers");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");
import docsIdsBasedOnQueryFetcher = require("viewmodels/database/patch/docsIdsBasedOnQueryFetcher");
import patchCommand = require("commands/database/patch/patchCommand");
import validationHelpers = require("viewmodels/common/validationHelpers");
import documentPreviewer = require("models/database/documents/documentPreviewer");
import queryUtil = require("common/queryUtil");
import getIndexesDefinitionsCommand = require("commands/database/index/getIndexesDefinitionsCommand");
import { highlight, languages } from "prismjs";

type testTabType = "preview" | "loaded" | "modified" | "deleted" | "output";

class patchTester extends viewModelBase {
    
    view: any = null;

    private db: KnockoutObservable<database>;

    testMode = ko.observable<boolean>(false);
    query: KnockoutObservable<string>;
    documentId = ko.observable<string>();

    beforeDoc = ko.observable<string>("");
    afterDoc = ko.observable<string>("");

    actions = {
        loadDocument: ko.observableArray<any>(),
        putDocument: ko.observableArray<any>(),
        deleteDocument: ko.observableArray<string>(),

        getCounter: ko.observableArray<any>(),
        incrementCounter: ko.observableArray<any>(),
        deleteCounter: ko.observableArray<string>(),

        getTimeSeries: ko.observableArray<any>(),
        appendTimeSeries: ko.observableArray<any>(),
        deleteTimeSeries: ko.observableArray<any>(),
        
        output: ko.observableArray<string>()
    };

    loadedCount: KnockoutComputed<string>;
    modifiedCount: KnockoutComputed<string>;
    deletedCount: KnockoutComputed<string>;
    
    showDocumentsInModified = ko.observable<boolean>(false);
    showTimeSeriesValuesInLoaded = ko.observable<boolean>(false);
    showTimeSeriesValuesInModified = ko.observable<boolean>(false);

    activeTestTab = ko.observable<testTabType>();
    
    spinners = {
        testing: ko.observable<boolean>(false),
        loadingDocument: ko.observable<boolean>(false),
        autocomplete: ko.observable<boolean>(false),
        preview: ko.observable<boolean>(false)
    };

    private $body = $('body');
    
    mapReduceIndexesCache = new Set<string>();

    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    docsIdsAutocompleteSource: docsIdsBasedOnQueryFetcher;

    validationGroup: KnockoutValidationGroup;
    testDocumentValidationGroup: KnockoutValidationGroup;

    constructor(query: KnockoutObservable<string>, db: KnockoutObservable<database>) {
        super();
        this.query = query;
        this.db = db;
        
        this.initObservables();

        this.bindToCurrentInstance(
            "closeTestMode", "enterTestMode", "runTest", "onAutocompleteOptionSelected");

        this.validationGroup = ko.validatedObservable({
            script: this.query,
            documentId: this.documentId
        });

        this.testDocumentValidationGroup = ko.validatedObservable({
            documentId: this.documentId
        });

        this.docsIdsAutocompleteSource = new docsIdsBasedOnQueryFetcher(this.db);
        
        new getIndexesDefinitionsCommand(this.db(), 0, 1024 * 1024)
            .execute()
            .then(indexes => {
                indexes.forEach(index => {
                    if (index.Type === "AutoMapReduce" || index.Type === "MapReduce" || index.Type === "JavaScriptMapReduce") {
                        this.mapReduceIndexesCache.add(index.Name.toLocaleLowerCase());
                    }
                })
            });
    }

    formatAsJson(input: KnockoutObservable<any> | any) {
        return ko.pureComputed(() => {
            const value = ko.unwrap(input);
            if (_.isUndefined(value)) {
                return "";
            } else {
                const json = JSON.stringify(value, null, 4);
                return highlight(json, languages.javascript, "js");
            }
        });
    }

    private initObservables() {
        this.testMode.subscribe(testMode => {
            this.$body.toggleClass('show-test', testMode);
        });

        this.documentId.extend({
            required: true
        });

        validationHelpers.addDocumentIdValidation(
            this.documentId, this.activeDatabase, ko.pureComputed(() => !!this.docsIdsAutocompleteResults()));

        this.query.subscribe(x => 
            this.docsIdsAutocompleteResults([]));

        this.documentId.throttle(250).subscribe(item => {
           if (!item) {
               this.resetForm();
               return;
           }

           const [name, type] = queryUtil.getCollectionOrIndexName(this.query());
           
           if (type === "index" && this.mapReduceIndexesCache.has(name.toLocaleLowerCase())) {
               // patch is not supported for map-reduce indexes
               messagePublisher.reportWarning("Patch operation is not supported for Map-Reduce indexes");
               return;
           }
           
           this.spinners.autocomplete(true);
           this.docsIdsAutocompleteSource.fetch(this.documentId(), this.query())
                .done(results => {
                    const term = item.toLowerCase();
                    results = _.take(results.filter(x => x.toLowerCase().indexOf(term) !== -1), 10);
                    this.docsIdsAutocompleteResults(results);
                })
                .always(() => this.spinners.autocomplete(false));
        });
        
        this.loadedCount = ko.pureComputed(() => {
            const actions = this.actions;

            const totalLoadedCount = actions.loadDocument().length +
                                     actions.getCounter().length +
                                     actions.getTimeSeries().length;

            return totalLoadedCount ? totalLoadedCount.toLocaleString() : "";
        });

        this.modifiedCount = ko.pureComputed(() => {
            const actions = this.actions;

            const totalModifiedCount = actions.putDocument().length +
                                       actions.incrementCounter().length +
                                       actions.appendTimeSeries().length;

            return totalModifiedCount ? totalModifiedCount.toLocaleString() : "";
        });

        this.deletedCount = ko.pureComputed(() => {
            const actions = this.actions;

            const totalDeletedCount = actions.deleteDocument().length +
                                      actions.deleteCounter().length +
                                      actions.deleteTimeSeries().length;

            return totalDeletedCount ? totalDeletedCount.toLocaleString() : "";
        });
    }

    closeTestMode() {
        this.testMode(false);
    }

    enterTestMode() {
        eventsCollector.default.reportEvent("patch", "test-mode");
        this.testMode(true);
        this.documentId("");
        this.resetForm();
        this.activeTestTab("preview");
        this.validationGroup.errors.showAllMessages(false);
    }

    resetForm() {
        this.actions.loadDocument([]);
        this.actions.putDocument([]);
        this.actions.deleteDocument([]);

        this.actions.getCounter([]);
        this.actions.incrementCounter([]);
        this.actions.deleteCounter([]);

        this.actions.getTimeSeries([]);
        this.actions.appendTimeSeries([]);
        this.actions.deleteTimeSeries([]);
        
        this.actions.output([]);
        this.afterDoc("");
        this.beforeDoc("");
    }

    loadDocument() {
        this.resetForm();

        this.spinners.loadingDocument(true);

        viewHelpers.asyncValidationCompleted(this.testDocumentValidationGroup)
        .then(() => {
            return new getDocumentWithMetadataCommand(this.documentId(), this.db())
            .execute()
            .done((doc: document) => {
                if (doc) {
                    const docDto = doc.toDto(true);
                    const metaDto = docDto["@metadata"];
                    documentMetadata.filterMetadata(metaDto);
                    this.beforeDoc(JSON.stringify(docDto, null, 4));
                }
            })
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    messagePublisher.reportWarning("Document doesn't exist.");
                } else {
                    messagePublisher.reportError("Failed to load document.", xhr.responseText, xhr.statusText);
                }
            })
            .always(() => this.spinners.loadingDocument(false));
        });
    }

    onAutocompleteOptionSelected(item: string) {
        this.documentId(item);
        this.loadDocument();
    }

    runTest(): void {
        eventsCollector.default.reportEvent("patch", "test");

        viewHelpers.asyncValidationCompleted(this.validationGroup, () => {
            if (this.isValid(this.validationGroup)) {
                this.spinners.testing(true);
                this.resetForm();

                const query = this.query();

                new patchCommand(query, this.db(), { test: true, documentId: this.documentId() })
                    .execute()
                    .done((result: any) => {
                        const modifiedDocument = new document(result.ModifiedDocument).toDto(true);
                        const originalDocument = new document(result.OriginalDocument).toDto(true);
                        
                        this.beforeDoc(JSON.stringify(originalDocument, null, 4));
                        this.afterDoc(JSON.stringify(modifiedDocument, null, 4));

                        const debug = result.Debug;
                        const actions = debug.Actions as Raven.Server.Documents.Patch.PatchDebugActions;
                        
                        this.actions.output(debug.Output);
                        
                        this.actions.loadDocument(actions.LoadDocument);
                        this.actions.putDocument(actions.PutDocument);
                        this.actions.deleteDocument(actions.DeleteDocument);
                        
                        this.actions.getCounter(actions.GetCounter);
                        this.actions.incrementCounter(actions.IncrementCounter);
                        this.actions.deleteCounter(actions.DeleteCounter);
                        
                        this.actions.getTimeSeries(actions.GetTimeSeries);
                        this.actions.appendTimeSeries(actions.AppendTimeSeries);
                        this.actions.deleteTimeSeries(actions.DeleteTimeSeries);

                        if (result.Status === "Patched") {
                            messagePublisher.reportSuccess("Test completed");
                        }
                    })
                    .always(() => this.spinners.testing(false));
            }
        });
    }

    previewDocument() {
        const spinner = this.spinners.preview;
        const documentId: KnockoutObservable<string> = this.documentId;
        const documentIdValidationGroup = this.testDocumentValidationGroup;
        const db = this.activeDatabase;

        documentPreviewer.preview(documentId, db, documentIdValidationGroup, spinner);
    }
    
    tabClicked(tab: testTabType) {
       this.activeTestTab(tab); 
    }
}

export = patchTester;
