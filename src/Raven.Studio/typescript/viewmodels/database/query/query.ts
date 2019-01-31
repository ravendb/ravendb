import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/database/query/deleteDocumentsMatchingQueryConfirm");
import querySyntax = require("viewmodels/database/query/querySyntax");
import deleteDocsMatchingQueryCommand = require("commands/database/documents/deleteDocsMatchingQueryCommand");
import notificationCenter = require("common/notifications/notificationCenter");
import queryCommand = require("commands/database/query/queryCommand");
import queryCompleter = require("common/queryCompleter");
import database = require("models/resources/database");
import querySort = require("models/database/query/querySort");
import document = require("models/database/documents/document");
import queryStatsDialog = require("viewmodels/database/query/queryStatsDialog");
import savedQueriesStorage = require("common/storage/savedQueriesStorage");
import queryUtil = require("common/queryUtil");
import eventsCollector = require("common/eventsCollector");
import queryCriteria = require("models/database/query/queryCriteria");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import showDataDialog = require("viewmodels/common/showDataDialog");
import endpoints = require("endpoints");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import explainQueryDialog = require("viewmodels/database/query/explainQueryDialog");
import explainQueryCommand = require("commands/database/index/explainQueryCommand");
import timingsChart = require("common/timingsChart");
import graphQueryResults = require("common/query/graphQueryResults");
import debugGraphOutputCommand = require("commands/database/query/debugGraphOutputCommand");

type queryResultTab = "results" | "explanations" | "timings" | "graph";

type stringSearchType = "Starts With" | "Ends With" | "Contains" | "Exact";

type rangeSearchType = "Numeric Double" | "Numeric Long" | "Alphabetical" | "Datetime";

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<document>>;

type explanationItem = {
    explanations: string[];
    id: string;
}

type highlightItem = {
    Key: string;
    Fragment: string;
}

class highlightSection {
    data = new Map<string, highlightItem[]>();
    fieldName = ko.observable<string>();
    totalCount = ko.observable<number>(0);
}

class perCollectionIncludes {
    name: string;
    total = ko.observable<number>(0);
    items = new Map<string, document>();
    
    constructor(name: string) {
        this.name = name;
    }
}


class query extends viewModelBase {

    static readonly recentQueryLimit = 6;
    static readonly recentKeyword = 'Recent Query';

    static readonly ContainerSelector = "#queryContainer";
    static readonly $body = $("body");

    static readonly SearchTypes: stringSearchType[] = ["Exact", "Starts With", "Ends With", "Contains"];
    static readonly RangeSearchTypes: rangeSearchType[] = ["Numeric Double", "Numeric Long", "Alphabetical", "Datetime"];
    static readonly SortTypes: querySortType[] = ["Ascending", "Descending", "Range Ascending", "Range Descending"];

    static lastQuery = new Map<string, string>();

    clientVersion = viewModelBase.clientVersion;

    hasAnySavedQuery = ko.pureComputed(() => this.savedQueries().length > 0);

    filteredQueries = ko.pureComputed(() => {
        let text = this.filters.searchText();

        if (!text) {
            return this.savedQueries();
        }

        text = text.toLowerCase();

        return this.savedQueries().filter(x => x.name.toLowerCase().includes(text));
    });

    filters = {
        searchText: ko.observable<string>()
    };

    previewItem = ko.observable<storedQueryDto>();
    
    timingsGraph = new timingsChart(".js-timings-container");
    graphQueryResults = new graphQueryResults(".js-graph-container");

    previewCode = ko.pureComputed(() => {
        const item = this.previewItem();
        if (!item) {
            return "";
        }

        return item.queryText;
    });

    inSaveMode = ko.observable<boolean>();
    
    querySaveName = ko.observable<string>();
    saveQueryValidationGroup: KnockoutValidationGroup;

    private gridController = ko.observable<virtualGridController<any>>();

    savedQueries = ko.observableArray<storedQueryDto>();

    indexes = ko.observableArray<Raven.Client.Documents.Operations.IndexInformation>();

    criteria = ko.observable<queryCriteria>(queryCriteria.empty());
    cacheEnabled = ko.observable<boolean>(true);

    private indexEntriesStateWasTrue: boolean = false; // Used to save current query settings when switching to a 'dynamic' index

    columnsSelector = new columnsSelector<document>();

    queryFetcher = ko.observable<fetcherType>();
    explanationsFetcher = ko.observable<fetcherType>();
    effectiveFetcher = this.queryFetcher;
    
    queryStats = ko.observable<Raven.Client.Documents.Queries.QueryResult<any, any>>();
    staleResult: KnockoutComputed<boolean>;
    fromCache = ko.observable<boolean>(false);
    originalRequestTime = ko.observable<number>();
    dirtyResult = ko.observable<boolean>();
    currentTab = ko.observable<queryResultTab | highlightSection | perCollectionIncludes>("results");
    totalResultsForUi = ko.observable<number>(0);
    hasMoreUnboundedResults = ko.observable<boolean>(false);

    includesCache = ko.observableArray<perCollectionIncludes>([]);
    highlightsCache = ko.observableArray<highlightSection>([]);
    explanationsCache = new Map<string, explanationItem>();
    totalExplanations = ko.observable<number>(0);
    timings = ko.observable<Raven.Client.Documents.Queries.Timings.QueryTimings>();

    canDeleteDocumentsMatchingQuery: KnockoutComputed<boolean>;
    isMapReduceIndex: KnockoutComputed<boolean>;
    isCollectionQuery: KnockoutComputed<boolean>;
    isGraphQuery: KnockoutComputed<boolean>;
    isDynamicQuery: KnockoutComputed<boolean>;
    isAutoIndex: KnockoutComputed<boolean>;

    private columnPreview = new columnPreviewPlugin<document>();

    hasEditableIndex: KnockoutComputed<boolean>;
    queryCompleter: queryCompleter;
    queryHasFocus = ko.observable<boolean>();

    editIndexUrl: KnockoutComputed<string>;
    indexPerformanceUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    visualizerUrl: KnockoutComputed<string>;
    rawJsonUrl = ko.observable<string>();
    csvUrl = ko.observable<string>();

    isLoading = ko.observable<boolean>(false);
    containsAsterixQuery: KnockoutComputed<boolean>; // query contains: *.* ?

    queriedIndex: KnockoutComputed<string>;
    queriedIndexLabel: KnockoutComputed<string>;
    queriedIndexDescription: KnockoutComputed<string>;

    queriedFieldsOnly = ko.observable<boolean>(false);
    queriedIndexEntries = ko.observable<boolean>(false);
    
    isEmptyFieldsResult = ko.observable<boolean>(false);
    
    showFanOutWarning = ko.observable<boolean>(false);

    $downloadForm: JQuery;

    /*TODO
    isTestIndex = ko.observable<boolean>(false);
    
    selectedResultIndices = ko.observableArray<number>();
    
    enableDeleteButton: KnockoutComputed<boolean>;
    warningText = ko.observable<string>();
    isWarning = ko.observable<boolean>(false);
    
    indexSuggestions = ko.observableArray<indexSuggestion>([]);
    showSuggestions: KnockoutComputed<boolean>;

    static queryGridSelector = "#queryResultsGrid";*/

    private hideSaveQueryHandler = (e: Event) => {
        if ($(e.target).closest(".query-save").length === 0) {
            this.inSaveMode(false);
        }
    };

    constructor() {
        super();

        this.queryCompleter = queryCompleter.remoteCompleter(this.activeDatabase, this.indexes, "Select");
        aceEditorBindingHandler.install();
        datePickerBindingHandler.install();

        this.initObservables();
        this.initValidation();

        this.bindToCurrentInstance("runRecentQuery", "previewQuery", "removeQuery", "useQuery", "useQueryItem", 
            "goToHighlightsTab", "goToIncludesTab", "goToGraphTab");
    }

    private initObservables() {
        this.queriedIndex = ko.pureComputed(() => {
            const stats = this.queryStats();
            if (!stats)
                return null;

            return stats.IndexName;
        });

        this.queriedIndexLabel = ko.pureComputed(() => {
            const indexName = this.queriedIndex();

            if (indexName === "AllDocs") {
                return "All Documents";
            }

            return indexName;
        });

        this.queriedIndexDescription = ko.pureComputed(() => {
            const indexName = this.queriedIndex();

            if (!indexName)
                return "";
                    
            if (indexName === "AllDocs") {
                return "All Documents";
            }

            const collectionRegex = /collection\/(.*)/;
            let m;
            if (m = indexName.match(collectionRegex)) {
                return m[1];
            }

            return indexName;
        });

        this.hasEditableIndex = ko.pureComputed(() => {
            const indexName = this.queriedIndex();
            if (!indexName)
                return false;

            return !indexName.startsWith(queryUtil.DynamicPrefix) &&
                indexName !== queryUtil.AllDocs;
        });

        this.editIndexUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forEditIndex(this.queriedIndex(), this.activeDatabase()) : null);

        this.indexPerformanceUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forIndexPerformance(this.activeDatabase(), this.queriedIndex()) : null);

        this.termsUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forTerms(this.queriedIndex(), this.activeDatabase()) : null);

        this.visualizerUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forVisualizer(this.activeDatabase(), this.queriedIndex()) : null);

        this.isMapReduceIndex = ko.pureComputed(() => {
            const indexName = this.queriedIndex();
            if (!indexName)
                return false;

            const indexes = this.indexes() || [];
            const currentIndex = indexes.find(i => i.Name === indexName);
            return !!currentIndex && (currentIndex.Type === "AutoMapReduce" || currentIndex.Type === "MapReduce" || currentIndex.Type === "JavaScriptMapReduce");
        });

        this.isCollectionQuery = ko.pureComputed(() => {
            const indexName = this.queriedIndex();
            if (!indexName)
                return false;

            if (!indexName.startsWith("collection/")) {
                return false;
            }
            
            // static index can have name starting with 'collection/' - let's check that as well
            const indexes = this.indexes() || [];
            return !indexes.find(x => x.Name === indexName);
        });
        
        this.isGraphQuery = ko.pureComputed(() => {
            const indexName = this.queriedIndex();
            return "@graph" === indexName;
        });
        
        this.isDynamicQuery = ko.pureComputed(() => {
            return queryUtil.isDynamicQuery(this.criteria().queryText());
        });
        
        this.isAutoIndex = ko.pureComputed(() => {
            const indexName = this.queriedIndex();
            if (!indexName)
                return false;
            
            return indexName.toLocaleLowerCase().startsWith(queryUtil.AutoPrefix);
        });

        this.canDeleteDocumentsMatchingQuery = ko.pureComputed(() => {
            const mapReduce = this.isMapReduceIndex();
            const graphQuery = this.isGraphQuery();
            return !mapReduce && !graphQuery;
        });

        this.containsAsterixQuery = ko.pureComputed(() => this.criteria().queryText().includes("*.*"));

        this.staleResult = ko.pureComputed(() => {
            const stats = this.queryStats();
            return stats ? stats.IsStale : false;
        });

        this.cacheEnabled.subscribe(() => {
            eventsCollector.default.reportEvent("query", "toggle-cache");
        });

        this.isLoading.extend({ rateLimit: 100 });

        const criteria = this.criteria();

        criteria.showFields.subscribe(() => this.runQuery());   
      
        criteria.indexEntries.subscribe((checked) => {
            if (checked && this.isCollectionQuery()) {
                criteria.indexEntries(false);
            } else {
                // run index entries option only if not dynamic index
                this.runQuery();
            }
        });

        criteria.name.extend({
            required: true
        });

         /* TODO
        this.showSuggestions = ko.computed<boolean>(() => {
            return this.indexSuggestions().length > 0;
        });

        this.selectedIndex.subscribe(index => this.onIndexChanged(index));
        });*/

        this.inSaveMode.subscribe(enabled => {
            const $input = $(".query-save .form-control");
            if (enabled) {
                $input.show();
                window.addEventListener("click", this.hideSaveQueryHandler, true);
            } else {
                this.saveQueryValidationGroup.errors.showAllMessages(false);
                window.removeEventListener("click", this.hideSaveQueryHandler, true);
                setTimeout(() => $input.hide(), 200);
            }
        });
        
        this.previewItem.extend({ rateLimit: 100}); 
        
        this.currentTab.subscribe(newTab => {
            if (newTab !== "graph") {
                this.graphQueryResults.stopSimulation();
            }
        })
    }

    private initValidation() {
        this.querySaveName.extend({
            required: true
        });
        
        this.saveQueryValidationGroup = ko.validatedObservable({
            querySaveName: this.querySaveName
        });
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                this.loadSavedQueries();

                return { can: true };
            }); 
    }

    activate(indexNameOrRecentQueryHash?: string) {
        super.activate(indexNameOrRecentQueryHash);

        this.updateHelpLink('KCIMJK');
        
        const db = this.activeDatabase();

        return this.fetchAllIndexes(db)
            .done(() => this.selectInitialQuery(indexNameOrRecentQueryHash));
    }

    deactivate(): void {
        super.deactivate();

        const queryText = this.criteria().queryText();

        this.saveLastQuery(queryText);
    }

    private saveLastQuery(queryText: string) {
        query.lastQuery.set(this.activeDatabase().name, queryText);
    }

    attached() {
        super.attached();

        this.createKeyboardShortcut("ctrl+enter", () => this.runQuery(), query.ContainerSelector);

        /* TODO
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+r", () => this.runQuery(), query.containerSelector); // Using keyboard shortcut here, rather than HTML's accesskey, so that we don't steal focus from the editor.
        */

        this.registerDisposableHandler($(window), "storage", () => this.loadSavedQueries());
    }

    compositionComplete() {
        super.compositionComplete();

        this.$downloadForm = $("#exportCsvForm");
        
        this.setupDisableReasons();

        const grid = this.gridController();

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), grid, {
            enableInlinePreview: true
        });

        const highlightingProvider = new documentBasedColumnsProvider(this.activeDatabase(), grid, {
            enableInlinePreview: false
        });

        if (!this.queryFetcher())
            this.queryFetcher(() => $.when({
                items: [] as document[],
                totalResultCount: 0
            }));
        
        this.explanationsFetcher(() => {
           const allExplanations = Array.from(this.explanationsCache.values());
           
           return $.when({
               items: allExplanations.map(x => new document(x)),
               totalResultCount: allExplanations.length
           });
        });
        
        this.columnsSelector.init(grid,
            (s, t, c) => this.effectiveFetcher()(s, t),
            (w, r) => {
                if (this.currentTab() === "results") {
                    return documentsProvider.findColumns(w, r);
                } else if (this.currentTab() === "explanations") {
                    return this.explanationsColumns(grid);
                } else {
                    return highlightingProvider.findColumns(w, r);
                }
            }, (results: pagedResult<document>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

        grid.headerVisible(true);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.queryFetcher.subscribe(() => grid.reset());

        this.columnPreview.install("virtual-grid", ".js-query-tooltip", 
            (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy: string) => void) => {
            if (this.currentTab() === "explanations" && column.header === "Explanation") {
                // we don't want to show inline preview for Explanation column, as value doesn't contain full message
                // which might be misleading - use preview button to obtain entire explanation 
                return;
            } 
            
            if (column instanceof textColumn) {
                const value = column.getCellValue(doc);
                if (!_.isUndefined(value)) {
                    const json = JSON.stringify(value, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html, json);
                }
            }
        });
        
        this.queryHasFocus(true);
    }
    
    private explanationsColumns(grid: virtualGridController<any>) {
        return [
            new actionColumn<explanationItem>(grid, doc => this.showExplanationDetails(doc), "Show", `<i class="icon-preview"></i>`, "72px",
            {
                title: () => 'Show detailed explanation'
            }),
            new textColumn<explanationItem>(grid, x => x.id, "Id", "30%"),
            new textColumn<explanationItem>(grid, x => x.explanations.map(x => x.split("\n")[0]).join(", "), "Explanation", "50%")
        ];
    }
    
    private showExplanationDetails(details: explanationItem) {
        app.showBootstrapDialog(new showDataDialog("Explanation for: " + details.id, details.explanations.join("\r\n"), "plain"));
    }

    private loadSavedQueries() {

        const db = this.activeDatabase();

        this.savedQueries(savedQueriesStorage.getSavedQueries(db));
        
        const myLastQuery = query.lastQuery.get(db.name);

        if (myLastQuery) {
            this.criteria().queryText(myLastQuery);
        }
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.indexes(results.Indexes);
            });
    }

    selectInitialQuery(indexNameOrRecentQueryHash: string) {
        if (!indexNameOrRecentQueryHash) {
            return;
        } else if (this.indexes().find(i => i.Name === indexNameOrRecentQueryHash) ||
            indexNameOrRecentQueryHash.startsWith(queryUtil.DynamicPrefix) || 
            indexNameOrRecentQueryHash === queryUtil.AllDocs) {
            this.runQueryOnIndex(indexNameOrRecentQueryHash);
        } else if (indexNameOrRecentQueryHash.indexOf("recentquery-") === 0) {
            const hash = parseInt(indexNameOrRecentQueryHash.substr("recentquery-".length), 10);
            const matchingQuery = this.savedQueries().find(q => q.hash === hash);
            if (matchingQuery) {
                this.runRecentQuery(matchingQuery);
            } else {
                this.navigate(appUrl.forQuery(this.activeDatabase()));
            }
        } else if (indexNameOrRecentQueryHash) {
            messagePublisher.reportError(`Could not find index or recent query: ${indexNameOrRecentQueryHash}`);
            // fallback to All Documents, but show error
            this.runQueryOnIndex(queryUtil.AllDocs);
        }
    }

    runQueryOnIndex(indexName: string) {
        this.criteria().setSelectedIndex(indexName);

        if (this.isCollectionQuery() && this.criteria().indexEntries()) {
            this.criteria().indexEntries(false);
            this.indexEntriesStateWasTrue = true; // save the state..
        }

        if (!this.isCollectionQuery() && this.indexEntriesStateWasTrue) {
            this.criteria().indexEntries(true);
            this.indexEntriesStateWasTrue = false;
        }

        this.runQuery();

        const url = appUrl.forQuery(this.activeDatabase(), indexName);
        this.updateUrl(url);
    }

    runQuery(optionalSavedQueryName?: string) {
        if (!this.isValid(this.criteria().validationGroup)) {
            return;
        }
        
        this.columnsSelector.reset();
        
        this.effectiveFetcher = this.queryFetcher;
        this.currentTab("results");
        this.includesCache.removeAll();
        this.highlightsCache.removeAll();
        this.explanationsCache.clear();
        this.timings(null);
        this.showFanOutWarning(false);
        
        this.isEmptyFieldsResult(false);
        
        eventsCollector.default.reportEvent("query", "run");
        const criteria = this.criteria();

        this.saveQueryOptions(criteria);
        
        const criteriaDto = criteria.toStorageDto();
        const disableCache = !this.cacheEnabled();

        if (criteria.queryText()) {
            this.isLoading(true);

            const database = this.activeDatabase();

            //TODO: this.currentColumnsParams().enabled(this.showFields() === false && this.indexEntries() === false);

            const queryCmd = new queryCommand(database, 0, 25, this.criteria(), disableCache);

            // we declare this variable here, if any result returns skippedResults <> 0 we enter infinite scroll mode 
            let totalSkippedResults = 0;
            let itemsSoFar = 0;
            
            try {
                this.rawJsonUrl(appUrl.forDatabaseQuery(database) + queryCmd.getUrl());
                this.csvUrl(queryCmd.getCsvUrl());    
            } catch (error) {
                // it may throw when unable to compute query parameters, etc.
                messagePublisher.reportError("Unable to run the query", error.message, null, false);
                this.isLoading(false);
                return;
            }
            
            const resultsFetcher = (skip: number, take: number) => {
                const command = new queryCommand(database, skip + totalSkippedResults, take + 1, this.criteria(), disableCache);
                
                const resultsTask = $.Deferred<pagedResultExtended<document>>();
                const queryForAllFields = this.criteria().showFields();
                                
                // Note: 
                // When server response is '304 Not Modified' then the browser cached data contains duration time from the 'first' execution  
                // If we ask browser to report the 304 state then 'response content' is empty 
                // This is why we need to measure the execution time here ourselves..
                const startQueryTime = new Date().getTime();                             
                
                command.execute()
                    .always(() => {
                        this.isLoading(false);
                    })
                    .done((queryResults: pagedResultExtended<document>) => {
                        this.hasMoreUnboundedResults(false);
                        
                        if (queryResults.items.length < take + 1) {
                            // we get less items than requested. I assume the distinct operation was used. 
                            // let's try to handle that. I assuming that we reach the end of results.
                            queryResults.totalResultCount = skip + queryResults.items.length;
                            queryResults.additionalResultInfo.TotalResults = queryResults.totalResultCount;
                        }

                        const totalFromQuery = queryResults.totalResultCount || 0;
                        
                        itemsSoFar += queryResults.items.length;
                        
                        this.totalResultsForUi(totalFromQuery);
                    
                        if (queryResults.additionalResultInfo.TotalResults === -1) {
                            // unbounded result set - startsWith() on collection 
                            if (queryResults.items.length === take + 1) {
                                // returned all or have more
                                const returnedLimit = queryResults.additionalResultInfo.CappedMaxResults || Number.MAX_SAFE_INTEGER;
                                this.hasMoreUnboundedResults(returnedLimit > itemsSoFar);
                                queryResults.totalResultCount = Math.min(skip + take + 30, returnedLimit - 1 /* subtract one since we fetch n+1 records */);
                            } else {
                                queryResults.totalResultCount = skip + queryResults.items.length;
                            }
                            queryResults.additionalResultInfo.TotalResults = queryResults.totalResultCount;
                            
                            this.totalResultsForUi(this.hasMoreUnboundedResults() ? itemsSoFar - 1 : itemsSoFar);
                        }
                        
                        if (queryResults.additionalResultInfo.SkippedResults) {
                            // apply skipped results (if any)
                            totalSkippedResults += queryResults.additionalResultInfo.SkippedResults;
                            
                            // find if query contains positive offset or limit, if so warn about paging. 
                            const [_, rqlWithoutParameters] = queryCommand.extractQueryParameters(this.criteria().queryText());
                            if (/\s+(offset|limit)\s+/img.test(rqlWithoutParameters)) {
                                this.showFanOutWarning(true);
                            }
                        }
                        
                        if (totalSkippedResults) {
                            queryResults.totalResultCount = skip + queryResults.items.length;
                            if (queryResults.items.length === take + 1) {
                                queryResults.totalResultCount += 30;
                                const totalWithOffsetAndLimit = queryResults.additionalResultInfo.CappedMaxResults;
                                if (totalWithOffsetAndLimit && totalWithOffsetAndLimit < queryResults.totalResultCount) { 
                                    queryResults.totalResultCount = totalWithOffsetAndLimit - 1;
                                }
                                
                                this.hasMoreUnboundedResults(itemsSoFar < totalFromQuery);
                            }
                            this.totalResultsForUi(this.hasMoreUnboundedResults() ? itemsSoFar - 1 : itemsSoFar);
                        }
                        
                        const endQueryTime = new Date().getTime();
                        const localQueryTime = endQueryTime - startQueryTime;
                        if (!disableCache && localQueryTime < queryResults.additionalResultInfo.DurationInMs) {
                            this.originalRequestTime(queryResults.additionalResultInfo.DurationInMs);
                            queryResults.additionalResultInfo.DurationInMs = localQueryTime;
                            this.fromCache(true);
                        } else {
                            this.originalRequestTime(null);
                            this.fromCache(false);
                        }
                        
                        const emptyFieldsResult = queryForAllFields 
                            && queryResults.totalResultCount > 0 
                            && _.every(queryResults.items, x => x.getDocumentPropertyNames().length === 0);
                        
                        if (emptyFieldsResult) {
                            resultsTask.resolve({
                               totalResultCount: 0,
                               includes: {},
                               items: [] 
                            });
                            this.isEmptyFieldsResult(true);
                            this.queryStats(queryResults.additionalResultInfo);
                            this.onHighlightingsLoaded({});
                        } else {
                            resultsTask.resolve(queryResults);
                            this.queryStats(queryResults.additionalResultInfo);
                            this.onIncludesLoaded(queryResults.includes);
                            this.onHighlightingsLoaded(queryResults.highlightings);
                            this.onExplanationsLoaded(queryResults.explanations);
                            this.onTimingsLoaded(queryResults.timings);
                        }
                        this.saveLastQuery("");
                        this.saveRecentQuery(criteriaDto, optionalSavedQueryName);
                        
                        this.setupDisableReasons(); 
                    })
                    .fail((request: JQueryXHR) => {
                        resultsTask.reject(request);
                    });
                
                return resultsTask;
            };

            this.queryFetcher(resultsFetcher);
            this.recordQueryRun(this.criteria());
        }
    }
    
    explainIndex() {
        new explainQueryCommand(this.criteria().queryText(), this.activeDatabase())
            .execute()
            .done(explanationResult => {
                app.showBootstrapDialog(new explainQueryDialog(explanationResult));
            });
    }

    saveQuery() {
        if (this.inSaveMode()) {
            eventsCollector.default.reportEvent("query", "save");

            if (this.isValid(this.saveQueryValidationGroup)) {
                
                // Verify if name already exists
                if (_.find(savedQueriesStorage.getSavedQueries(this.activeDatabase()), x => x.name.toUpperCase() === this.querySaveName().toUpperCase())) {
                    this.confirmationMessage(`Query ${this.querySaveName()} already exists`, `Overwrite existing query ?`, ["No", "Overwrite"])
                        .done(result => {
                            if (result.can) {
                                this.saveQueryToStorage(this.criteria().toStorageDto());   
                            }
                        });  
                } else {
                    this.saveQueryToStorage(this.criteria().toStorageDto());
                }
            }
        } else {
            if (this.isValid(this.criteria().validationGroup)) {
                this.inSaveMode(true);
            }
        }
    }
    
    private saveQueryOptions(criteria: queryCriteria) {
        this.queriedFieldsOnly(criteria.showFields());
        this.queriedIndexEntries(criteria.indexEntries());
    }
    
    private saveQueryToStorage(criteria: storedQueryDto) {
        criteria.name = this.querySaveName();
        this.saveQueryInStorage(criteria, false);
        this.querySaveName(null);
        this.saveQueryValidationGroup.errors.showAllMessages(false);
        messagePublisher.reportSuccess("Query saved successfully");
    }

    private saveRecentQuery(criteria: storedQueryDto, optionalSavedQueryName?: string) {
        const name = optionalSavedQueryName || this.getRecentQueryName();
        criteria.name = name;
        this.saveQueryInStorage(criteria, !optionalSavedQueryName);
    }

    private saveQueryInStorage(criteria: storedQueryDto, isRecent: boolean) {
        criteria.recentQuery = isRecent;
        this.appendQuery(criteria);
        savedQueriesStorage.storeSavedQueries(this.activeDatabase(), this.savedQueries());

        this.criteria().name("");
        this.loadSavedQueries();
    }

    showFirstItemInPreviewArea() {
        this.previewItem(savedQueriesStorage.getSavedQueries(this.activeDatabase())[0]);
    }
    
    appendQuery(doc: storedQueryDto) {
        if (doc.recentQuery) {
            const existing = this.savedQueries().find(query => query.hash === doc.hash);
            if (existing) {
                this.savedQueries.remove(existing);
                this.savedQueries.unshift(doc);
            } else {
                this.removeLastRecentQueryIfMoreThanLimit();
                this.savedQueries.unshift(doc);
            }
        } else {
            const existing = this.savedQueries().find(x => x.name === doc.name);
            
            if (existing) {
                this.savedQueries.remove(existing);                    
            }

            this.savedQueries.unshift(doc);
        }
    }

    private removeLastRecentQueryIfMoreThanLimit() {
        this.savedQueries()
            .filter(x => x.recentQuery)
            .filter((_, idx) => idx >= query.recentQueryLimit)
            .forEach(x => this.savedQueries.remove(x));
    }

    private getRecentQueryName(): string {
        const [collectionIndexName, type] = queryUtil.getCollectionOrIndexName(this.criteria().queryText());
        return type !== "unknown" ? query.recentKeyword + " (" + collectionIndexName + ")" : query.recentKeyword;
    }

    previewQuery(item: storedQueryDto) {
        this.previewItem(item);
    }

    useQueryItem(item: storedQueryDto) {
        this.previewItem(item);
        this.useQuery();
    }

    useQuery() {
        const queryDoc = this.criteria();
        const previewItem  = this.previewItem();
        queryDoc.copyFrom(previewItem);
        
        // Reset settings
        this.cacheEnabled(true);
        this.criteria().indexEntries(false);
        this.criteria().showFields(false);
        
        this.runQuery(previewItem.recentQuery ? null : previewItem.name);
    }

    removeQuery(item: storedQueryDto) {
        this.confirmationMessage("Query", `Are you sure you want to delete query '${item.name}'?`, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {

                    if (this.previewItem() === item) {
                        this.previewItem(null);
                    }

                    savedQueriesStorage.removeSavedQueryByHash(this.activeDatabase(), item.hash);
                    this.loadSavedQueries();
                }
            });
    }
    
    private onExplanationsLoaded(explanations: dictionary<Array<string>>) {
        _.forIn(explanations, (doc, id) => {
            this.explanationsCache.set(id, {
               id: id,
                explanations: doc
            });
        });
        
        this.totalExplanations(this.explanationsCache.size);
    }
    
    private onTimingsLoaded(timings: Raven.Client.Documents.Queries.Timings.QueryTimings) {
        this.timings(timings);
    }
    
    private onIncludesLoaded(includes: dictionary<any>) {
        _.forIn(includes, (doc, id) => {
            const metadata = doc['@metadata'];
            const collection = (metadata ? metadata["@collection"] : null) || "@unknown";
            
            let perCollectionCache = this.includesCache().find(x => x.name === collection);
            if (!perCollectionCache) {
                perCollectionCache = new perCollectionIncludes(collection);
                this.includesCache.push(perCollectionCache);
            }
            perCollectionCache.items.set(id, doc);
        });
        
        this.includesCache().forEach(cache => {
            cache.total(cache.items.size);
        });
    }
    
    private onHighlightingsLoaded(highlightings: dictionary<dictionary<Array<string>>>) {
        _.forIn(highlightings, (value, fieldName) => {
            let existingPerFieldCache = this.highlightsCache().find(x => x.fieldName() === fieldName);

            if (!existingPerFieldCache) {
                existingPerFieldCache = new highlightSection();
                existingPerFieldCache.fieldName(fieldName);
                this.highlightsCache.push(existingPerFieldCache);
            }
            
            _.forIn(value, (fragments, key) => {
               if (!existingPerFieldCache.data.has(key)) {
                   existingPerFieldCache.data.set(key, [] as Array<highlightItem>);
               } 
               const existingFragments = existingPerFieldCache.data.get(key);
               
               fragments.forEach(fragment => {
                   existingFragments.push({
                       Key: key,
                       Fragment: fragment
                   });
                   
                   existingPerFieldCache.totalCount(existingPerFieldCache.totalCount() + 1);
               });
            });
        });
    }

    refresh() {
        this.gridController().reset(true);
    }
    
    openQueryStats() {
        //TODO: work on explain in dialog
        eventsCollector.default.reportEvent("query", "show-stats");
        const totalResultsFormatted = this.totalResultsForUi().toLocaleString() + (this.hasMoreUnboundedResults() ? "+" : "");
        const viewModel = new queryStatsDialog(this.queryStats(), totalResultsFormatted, this.activeDatabase());
        app.showBootstrapDialog(viewModel);
    }

    private recordQueryRun(criteria: queryCriteria) {
        const newQuery: storedQueryDto = criteria.toStorageDto();

        const queryUrl = appUrl.forQuery(this.activeDatabase(), newQuery.hash);
        this.updateUrl(queryUrl);        
    }

    runRecentQuery(storedQuery: storedQueryDto) {
        eventsCollector.default.reportEvent("query", "run-recent");

        const criteria = this.criteria();

        criteria.updateUsing(storedQuery);

        this.runQuery();
    }

    getRecentQuerySortText(sorts: string[]) {
        if (sorts.length > 0) {
            return sorts
                .map(s => querySort.fromQuerySortString(s).toHumanizedString())
                .join(", ");
        }

        return "";
    }

    deleteDocsMatchingQuery() {
        eventsCollector.default.reportEvent("query", "delete-documents");
        // Run the query so that we have an idea of what we'll be deleting.
        this.runQuery();
        this.queryFetcher()(0, 1)
            .done((results) => {
                if (results.totalResultCount === 0) {
                    app.showBootstrapMessage("There are no documents matching your query.", "Nothing to do");
                } else {
                    this.promptDeleteDocsMatchingQuery(results.totalResultCount);
                }
            });
    }

    private promptDeleteDocsMatchingQuery(resultCount: number) {
        const criteria = this.criteria();
        const db = this.activeDatabase();
        const viewModel = new deleteDocumentsMatchingQueryConfirm(this.queriedIndex(), criteria.queryText(), resultCount, db);
        app.showBootstrapDialog(viewModel)
           .done((result) => {
                if (result) {
                    new deleteDocsMatchingQueryCommand(criteria.queryText(), this.activeDatabase())
                        .execute()
                        .done((operationId: operationIdDto) => {
                            this.monitorDeleteOperation(db, operationId.OperationId);
                        });
                }
           });
    }

    syntaxHelp() {
        const viewmodel = new querySyntax();
        app.showBootstrapDialog(viewmodel);
    }

    private monitorDeleteOperation(db: database, operationId: number) {
        notificationCenter.instance.openDetailsForOperationById(db, operationId);

        notificationCenter.instance.monitorOperation(db, operationId)
            .done(() => {
                messagePublisher.reportSuccess("Successfully deleted documents");
                this.refresh();
            })
            .fail((exception: Raven.Client.Documents.Operations.OperationExceptionResult) => {
                messagePublisher.reportError("Could not delete documents: " + exception.Message, exception.Error, null, false);
            });
    }
    
    goToResultsTab() {
        this.currentTab("results");
        this.effectiveFetcher = this.queryFetcher;

        // since we merge records based on fragments
        // remove all existing highlights when going back to 
        // 'results' tab
        this.highlightsCache.removeAll();
        
        this.columnsSelector.reset();
        this.refresh();
    }
    
    goToIncludesTab(includes: perCollectionIncludes) {
        this.currentTab(includes);
        
        this.effectiveFetcher = ko.observable<fetcherType>(() => {
            return $.when({
                items: Array.from(includes.items.values()).map(x => new document(x)),
                totalResultCount: includes.total()
            });
        });

        this.columnsSelector.reset();
        this.refresh();
    }
    
    goToExplanationsTab() {
        this.currentTab("explanations");
        this.effectiveFetcher = this.explanationsFetcher;
        
        this.columnsSelector.reset();
        this.refresh();
    }

    goToHighlightsTab(highlight: highlightSection) {
        this.currentTab(highlight);

        const itemsFlattened = _.flatMap(Array.from(highlight.data.values()), items => items);
        
        this.effectiveFetcher = ko.observable<fetcherType>(() => {
            return $.when({
                items: itemsFlattened.map(x => new document(x)),
                totalResultCount: itemsFlattened.length
            });
        });
        this.columnsSelector.reset();
        this.refresh();
    }

    goToTimingsTab() {
        this.currentTab("timings");
        
        this.timingsGraph.draw(this.timings());
    }
    
    goToGraphTab() {
        this.currentTab("graph");
        
        //TODO: add spinner? 
        
        new debugGraphOutputCommand(this.activeDatabase(), this.criteria().queryText())
            .execute()
            .done((result) => {
                this.graphQueryResults.draw(result);
            });
    }

    exportCsv() {
        eventsCollector.default.reportEvent("query", "export-csv");

        let args: { format: string, debug?: string };
        if (this.criteria().indexEntries()) {
            args = {
                format: "csv",
                debug: "entries"
            };
        } else {
            args = {
                format: "csv"
            };
        }
        let payload: { Query: string };
        if (this.criteria().showFields()) {
            payload = {
                Query: queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(this.criteria().queryText())
            };
        } else {
            payload = {
                Query: this.criteria().queryText()
            };
        }
        $("input[name=ExportOptions]").val(JSON.stringify(payload));

        const url = appUrl.forDatabaseQuery(this.activeDatabase()) + endpoints.databases.streaming.streamsQueries + appUrl.urlEncodeArgs(args);
        this.$downloadForm.attr("action", url);
        this.$downloadForm.submit();
    }
}
export = query;
