import indexDefinition = require("models/database/index/indexDefinition");
import testIndexCommand = require("commands/database/index/testIndexCommand");
import database = require("models/resources/database");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentObject = require("models/database/documents/document");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import eventsCollector = require("common/eventsCollector");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import TestIndexResult = Raven.Server.Documents.Indexes.Test.TestIndexResult;
import assertUnreachable = require("components/utils/assertUnreachable");
import prismjs = require("prismjs");
import rqlLanguageService = require("common/rqlLanguageService");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

type testTabName = "queryResults" | "indexEntries" | "mapResults" | "reduceResults";
type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

const unnamedIndexQuery = `from index "<TestIndexName>"`;

class testIndex {
    spinners = {
        testing: ko.observable<boolean>(false)
    };

    private readonly dbProvider: () => database;

    testTimeLimit = ko.observable<number>();
    testScanLimit = ko.observable<number>(10_000);
    query = ko.observable<string>(unnamedIndexQuery);

    gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector.default<documentObject>();
    fetchTask: JQueryPromise<TestIndexResult>;
    resultsFetcher = ko.observable<fetcherType>();
    effectiveFetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();

    isFirstRun = true;

    resultsCount = ko.observable<Record<testTabName, number>>({
        "queryResults": 0,
        "indexEntries": 0,
        "mapResults": 0,
        "reduceResults": 0
    });
    
    showReduceTab = ko.observable<boolean>(false);

    currentTab = ko.observable<testTabName>(null);

    languageService: rqlLanguageService;

    constructor(dbProvider: () => database) {
        this.dbProvider = dbProvider;
        aceEditorBindingHandler.install();

        this.languageService = new rqlLanguageService(dbProvider(), ko.observableArray([]), "Select"); // we intentionally pass empty indexes here as subscriptions works only on collections
    }

    compositionComplete() {
        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));

        this.query.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }

    toDto(indexDef: indexDefinition): Raven.Server.Documents.Indexes.Test.TestIndexParameters {
        return {
            IndexDefinition: indexDef.toDto(),
            WaitForNonStaleResultsTimeoutInSec: this.testTimeLimit() ?? 15,
            Query: this.query(),
            QueryParameters: null,
            MaxDocumentsToProcess: this.testScanLimit() ?? 10_000
        }
    }

    goToTab(tabToUse: testTabName) {
        this.currentTab(tabToUse);

        switch (tabToUse) {
            case "queryResults": {
                this.effectiveFetcher(() => this.fetchTask.then((result): pagedResult<any> => {
                    return {
                        items: result.QueryResults.map(x => new documentObject(x)),
                        totalResultCount: result.QueryResults.length
                    }
                }));
                return;
            }
            case "indexEntries": {
                this.effectiveFetcher(() => this.fetchTask.then((result): pagedResult<any> => {
                    return {
                        items: result.IndexEntries.map(x => new documentObject(x)),
                        totalResultCount: result.IndexEntries.length
                    }
                }));
                return;
            }
            case "mapResults": {
                this.effectiveFetcher(() => this.fetchTask.then((result): pagedResult<any> => {
                    return {
                        items: result.MapResults.map(x => new documentObject(x)),
                        totalResultCount: result.MapResults.length
                    }
                }));
                return;
            }
            case "reduceResults": {
                this.effectiveFetcher(() => this.fetchTask.then((result): pagedResult<any> => {
                    return {
                        items: result.ReduceResults.map(x => new documentObject(x)),
                        totalResultCount: result.ReduceResults.length
                    }
                }));
                return;
            }
            default:
                assertUnreachable.default(tabToUse);
        }
    }

    runTest(location: databaseLocationSpecifier, indexDef: indexDefinition) {
        const db = this.dbProvider();

        eventsCollector.default.reportEvent("index", "test");

        this.columnsSelector.reset();

        this.fetchTask = this.fetchTestDocuments(db, location, indexDef);

        this.goToTab("queryResults");

        if (this.isFirstRun) {
            const documentsProvider = new documentBasedColumnsProvider(db, this.gridController(), {
                showRowSelectionCheckbox: false,
                showSelectAllCheckbox: false,
                enableInlinePreview: true,
                createHyperlinks: false,
                customInlinePreview: doc => documentBasedColumnsProvider.showPreview(doc, "Entry preview")
            });

            this.columnsSelector.init(this.gridController(),
                (s, t) => this.effectiveFetcher()(s, t),
                (w, r) => documentsProvider.findColumns(w, r, ["__metadata"]),
                (results: pagedResult<documentObject>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

            const grid = this.gridController();
            grid.headerVisible(true);

            this.columnPreview.install("virtual-grid", ".js-index-test-tooltip", 
                (doc: documentObject, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
                if (column instanceof textColumn) {
                    const value = column.getCellValue(doc);
                    if (value !== undefined) {
                        const json = JSON.stringify(value, null, 4);
                        const html = prismjs.highlight(json, prismjs.languages.javascript, "js");
                        onValue(html, json);
                    }
                }
            });

            this.effectiveFetcher.subscribe(() => {
                this.columnsSelector.reset();
                grid.reset();
            });
        }

        this.isFirstRun = false;
    }

    dispose() {
        this.languageService.dispose();
    }

    private fetchTestDocuments(db: database, location: databaseLocationSpecifier, indexDef: indexDefinition): JQueryPromise<TestIndexResult> {
        this.spinners.testing(true);

        const dto = this.toDto(indexDef);

        return new testIndexCommand(dto, db, location).execute()
            .done(result => {
                this.resultsCount({
                    queryResults: result.QueryResults.length,
                    indexEntries: result.IndexEntries.length,
                    mapResults: result.MapResults.length,
                    reduceResults: result.ReduceResults.length
                });
                this.showReduceTab(result.IndexType === "AutoMapReduce" || result.IndexType === "MapReduce" || result.IndexType === "JavaScriptMapReduce");
            })
            .fail(() => {
                // reset results count
                this.resultsCount({
                    queryResults: 0,
                    reduceResults: 0,
                    mapResults: 0,
                    indexEntries: 0
                });
                this.showReduceTab(false);
            })
            .always(() => this.spinners.testing(false));
    }
}


export = testIndex;
