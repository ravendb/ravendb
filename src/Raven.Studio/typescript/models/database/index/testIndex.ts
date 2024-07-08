import indexDefinition from "models/database/index/indexDefinition";
import testIndexCommand from "commands/database/index/testIndexCommand";
import database from "models/resources/database";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import documentObject = require("models/database/documents/document");
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import columnsSelector = require("viewmodels/partial/columnsSelector");
import eventsCollector from "common/eventsCollector";
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import textColumn from "widgets/virtualGrid/columns/textColumn";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import TestIndexResult = Raven.Server.Documents.Indexes.Test.TestIndexResult;
import assertUnreachable from "components/utils/assertUnreachable";
import { highlight, languages } from "prismjs";
import rqlLanguageService from "common/rqlLanguageService";
import aceEditorBindingHandler from "common/bindingHelpers/aceEditorBindingHandler";

type testTabName = "queryResults" | "indexEntries" | "mapResults" | "reduceResults";
type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

const unnamedIndexQuery = `from index "<TestIndexName>"`;

class testIndex {
    spinners = {
        testing: ko.observable<boolean>(false)
    };

    private readonly indexDefinitionProvider: () => indexDefinition;
    private readonly dbProvider: () => database;

    testTimeLimit = ko.observable<number>();
    testScanLimit = ko.observable<number>(10_000);
    query = ko.observable<string>(unnamedIndexQuery);

    gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector<documentObject>();
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

    constructor(dbProvider: () => database, indexDefinitionProvider: () => indexDefinition) {
        this.dbProvider = dbProvider;
        this.indexDefinitionProvider = indexDefinitionProvider;

        if (indexDefinitionProvider().name()) {
            this.query(`from index "${indexDefinitionProvider().name()}"`)
        }

        aceEditorBindingHandler.install();

        this.languageService = new rqlLanguageService(dbProvider(), ko.observableArray([]), "Select"); // we intentionally pass empty indexes here as subscriptions works only on collections
    }

    compositionComplete() {
        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));

        this.query.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }

    toDto(): Raven.Server.Documents.Indexes.Test.TestIndexParameters {
        return {
            IndexDefinition: this.indexDefinitionProvider().toDto(),
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
                assertUnreachable(tabToUse);
        }
    }

    runTest(location: databaseLocationSpecifier) {
        const db = this.dbProvider();

        eventsCollector.default.reportEvent("index", "test");

        this.columnsSelector.reset();

        this.fetchTask = this.fetchTestDocuments(db, location);

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
                    if (!_.isUndefined(value)) {
                        const json = JSON.stringify(value, null, 4);
                        const html = highlight(json, languages.javascript, "js");
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

    private fetchTestDocuments(db: database, location: databaseLocationSpecifier): JQueryPromise<TestIndexResult> {
        this.spinners.testing(true);

        const dto = this.toDto();

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
