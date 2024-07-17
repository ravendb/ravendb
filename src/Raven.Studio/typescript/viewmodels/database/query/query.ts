import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/database/query/deleteDocumentsMatchingQueryConfirm");
import querySyntax = require("viewmodels/database/query/querySyntax");
import deleteDocsMatchingQueryCommand = require("commands/database/documents/deleteDocsMatchingQueryCommand");
import notificationCenter = require("common/notifications/notificationCenter");
import queryCommand = require("commands/database/query/queryCommand");
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
import generalUtils = require("common/generalUtils");
import timeSeriesColumn = require("widgets/virtualGrid/columns/timeSeriesColumn");
import timeSeriesPlotDetails = require("viewmodels/common/timeSeriesPlotDetails");
import timeSeriesQueryResult = require("models/database/timeSeries/timeSeriesQueryResult");
import spatialMarkersLayerModel = require("models/database/query/spatialMarkersLayerModel");
import spatialQueryMap = require("viewmodels/database/query/spatialQueryMap");
import popoverUtils = require("common/popoverUtils");
import spatialCircleModel = require("models/database/query/spatialCircleModel");
import spatialPolygonModel = require("models/database/query/spatialPolygonModel");
import rqlLanguageService = require("common/rqlLanguageService");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import moment = require("moment");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import killQueryCommand from "commands/database/query/killQueryCommand";
import getEssentialDatabaseStatsCommand from "commands/resources/getEssentialDatabaseStatsCommand";
import queryPlan from "viewmodels/database/query/queryPlan";

type queryResultTab = "results" | "explanations" | "queryPlan" | "timings" | "revisions";

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

class timeSeriesTableDetails {
    
    tabText: KnockoutComputed<string>;
    tabInfo: KnockoutComputed<string>;
    
    constructor(public documentId: string, public name: string, public value: timeSeriesQueryResultDto) {

        this.tabText = ko.pureComputed(() => {
            return `TS - ${generalUtils.truncateDocumentId(this.documentId)}`;
        });

        this.tabInfo = ko.pureComputed(() => {
            return `<div class="tab-info-tooltip padding padding-sm">
                       <div class="margin-bottom margin-bottom-sm"><strong>Time Series Table for:</strong></div>
                       <div class="text-left document-id">${generalUtils.escapeHtml(this.documentId)}</div>
                   </div>`;
        });
    }
    
    isEqual(table: timeSeriesTableDetails) {
        if (table.documentId !== this.documentId) {
            return false;
        }
        
        if (table.name !== this.name) {
            return false;
        }
        
        return true;
    }
}

class perCollectionIncludes {
    name: string;
    total = ko.observable<number>(0);
    items = new Map<string, document>();
    
    constructor(name: string) {
        this.name = name;
    }
}

type includedRevisionItem = {
    changeVector: string;
    sourceDocument: string;
    lastModified: string;
    revision: string;
}

class includedRevisions {
    total = ko.observable<number>(0);
    items: Array<includedRevisionItem> = [];
}

class query extends shardViewModelBase {

    static readonly clientQueryId = "studio_" + new Date().getTime();
    static readonly dateTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";

    view = require("views/database/query/query.html");

    static readonly recentQueryLimit = 6;
    static readonly recentKeyword = 'Recent Query';

    static readonly ContainerSelector = "#queryContainer";
    static readonly $body = $("body");
    static readonly SaveQuerySelector = ".query-save";

    static readonly SearchTypes: stringSearchType[] = ["Exact", "Starts With", "Ends With", "Contains"];
    static readonly RangeSearchTypes: rangeSearchType[] = ["Numeric Double", "Numeric Long", "Alphabetical", "Datetime"];
    static readonly SortTypes: querySortType[] = ["Ascending", "Descending", "Range Ascending", "Range Descending"];

    static lastQueryNotExecuted = new Map<string, string>();

    static readonly maxSpatialResultsToFetch = 5000;
    
    autoOpenGraph = false;

    saveQueryFocus = ko.observable<boolean>(false);

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
    
    resultsExpanded = ko.observable<boolean>(false);

    previewCode = ko.pureComputed(() => {
        const item = this.previewItem();
        if (!item) {
            return "";
        }

        return item.queryText;
    });

    inSaveMode = ko.observable<boolean>();
    
    showKillQueryButton = ko.observable<boolean>(false);
    killQueryTimeoutId: ReturnType<typeof setTimeout>;
    
    querySaveName = ko.observable<string>();
    saveQueryValidationGroup: KnockoutValidationGroup;

    private gridController = ko.observable<virtualGridController<any>>();

    savedQueries = ko.observableArray<storedQueryDto>();

    indexes = ko.observableArray<Raven.Client.Documents.Operations.EssentialIndexInformation>();

    criteria = ko.observable<queryCriteria>(queryCriteria.empty());
    lastCriteriaExecuted: queryCriteria = queryCriteria.empty();
    
    cacheEnabled = ko.observable<boolean>(true);
    disableAutoIndexCreation = ko.observable<boolean>(true);
    projectionBehavior = ko.observable<Raven.Client.Documents.Queries.ProjectionBehavior>("Default");
    
    private indexEntriesStateWasTrue = false; // Used to save current query settings when switching to a 'dynamic' index

    columnsSelector = new columnsSelector<document>();

    queryFetcher = ko.observable<fetcherType>();
    explanationsFetcher = ko.observable<fetcherType>();
    effectiveFetcher = this.queryFetcher;
    includedRevisionsFetcher = ko.observable<fetcherType>();
    
    queryPlanGraph = new queryPlan();
    
    queryStats = ko.observable<Raven.Client.Documents.Queries.QueryResult<any, any>>();
    staleResult: KnockoutComputed<boolean>;
    fromCache = ko.observable<boolean>(false);
    originalRequestTime = ko.observable<number>();
    dirtyResult = ko.observable<boolean>();
    currentTab = ko.observable<queryResultTab | highlightSection | perCollectionIncludes | timeSeriesPlotDetails | timeSeriesTableDetails | spatialQueryMap>("results");
    totalResultsForUi = ko.observable<number>(0);
    hasMoreUnboundedResults = ko.observable<boolean>(false);

    includesCache = ko.observableArray<perCollectionIncludes>([]);
    includesRevisionsCache = ko.observable<includedRevisions>(new includedRevisions());
    highlightsCache = ko.observableArray<highlightSection>([]);
    explanationsCache: explanationItem[] = [];
    totalExplanations = ko.observable<number>(0);
    timings = ko.observable<Raven.Client.Documents.Queries.Timings.QueryTimings>();
    
    queryPlan = ko.observable<Raven.Client.Documents.Queries.Timings.QueryInspectionNode>();

    canDeleteDocumentsMatchingQuery: KnockoutComputed<boolean>;
    deleteDocumentDisableReason: KnockoutComputed<string>;
    canExportToFile: KnockoutComputed<boolean>;
    
    isMapReduceIndex: KnockoutComputed<boolean>;
    isCollectionQuery: KnockoutComputed<boolean>;
    isDynamicQuery: KnockoutComputed<boolean>;
    isAutoIndex: KnockoutComputed<boolean>;
    
    showVirtualTable: KnockoutComputed<boolean>;
    showTimeSeriesGraph: KnockoutComputed<boolean>;
    showPlotButton: KnockoutComputed<boolean>;
    
    isSpatialQuery = ko.observable<boolean>();
    spatialMap = ko.observable<spatialQueryMap>();
    showMapView: KnockoutComputed<boolean>;
    numberOfMarkers = ko.observable<number>(0);
    numberOfMarkersText: KnockoutComputed<string>;
    spatialResultsOnMapText: KnockoutComputed<string>;
    hasMoreSpatialResultsForMap = ko.observable<boolean>(false);
    allSpatialResultsItems = ko.observableArray<document>([]);
    failedToGetResultsForSpatial = ko.observable<boolean>(false);
        
    timeSeriesGraphs = ko.observableArray<timeSeriesPlotDetails>([]);
    timeSeriesTables = ko.observableArray<timeSeriesTableDetails>([]);

    private columnPreview = new columnPreviewPlugin<document>();

    hasEditableIndex: KnockoutComputed<boolean>;
    languageService: rqlLanguageService;
    queryHasFocus = ko.observable<boolean>();

    editIndexUrl: KnockoutComputed<string>;
    indexPerformanceUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    visualizerUrl: KnockoutComputed<string>;
    rawJsonUrl = ko.observable<string>();

    containsAsterixQuery: KnockoutComputed<boolean>; // query contains: *.* ?

    queriedIndex: KnockoutComputed<string>;
    queriedIndexInfo = ko.observable<Raven.Client.Documents.Operations.EssentialIndexInformation>();
    
    queriedIndexLabel: KnockoutComputed<string>;
    queriedIndexDescription: KnockoutComputed<string>;

    queriedFieldsOnly = ko.observable<boolean>(false);
    queriedIndexEntries = ko.observable<boolean>(false);
    
    queryResultsContainMatchingDocuments: KnockoutComputed<boolean>;
    
    isEmptyFieldsResult = ko.observable<boolean>(false);
    
    showFanOutWarning = ko.observable<boolean>(false);

    $downloadForm: JQuery;
    
    spinners = {
        isLoadingSpatialResults: ko.observable<boolean>(false),
        isLoading: ko.observable<boolean>(false)
    };
    
    exportAsFileSettings = {
        format: ko.observable<"json" | "csv">("csv"),
        allColumns: ko.observable<boolean>(true)
    }

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

    constructor(db: database) {
        super(db); 

        this.languageService = new rqlLanguageService(this.db, ko.pureComputed(() => this.indexes().map(x => x.Name)), "Select");
        
        aceEditorBindingHandler.install();
        datePickerBindingHandler.install();

        this.initObservables();
        this.initValidation();

        this.bindToCurrentInstance("runRecentQuery", "previewQuery", "removeQuery", "useQuery", "useQueryItem", 
            "goToHighlightsTab", "goToIncludesTab", "toggleResults", "goToTimeSeriesTab", "plotTimeSeries",
            "closeTimeSeriesTab", "goToSpatialMapTab", "loadMoreSpatialResultsToMap", "goToIncludesRevisionsTab",
            "killQuery");
    }

    private initObservables(): void {
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
            const m = indexName.match(collectionRegex);
            if (m) {
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
            this.queriedIndex() ? appUrl.forEditIndex(this.queriedIndex(), this.db) : null);

        this.indexPerformanceUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forIndexPerformance(this.db, this.queriedIndex()) : null);

        this.termsUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forTerms(this.queriedIndex(), this.db) : null);

        this.visualizerUrl = ko.pureComputed(() =>
            this.queriedIndex() ? appUrl.forVisualizer(this.db, this.queriedIndex()) : null);

        this.isMapReduceIndex = ko.pureComputed(() => {
            const currentIndex = this.queriedIndexInfo();
            return !!currentIndex && (currentIndex.Type === "AutoMapReduce" || currentIndex.Type === "MapReduce" || currentIndex.Type === "JavaScriptMapReduce");
        });

        this.queryResultsContainMatchingDocuments = ko.pureComputed(() => {
            if (this.isCollectionQuery()) {
                return true;
            }
            
            const currentIndex = this.queriedIndexInfo();
            return !!currentIndex && currentIndex.SourceType === "Documents" && !this.isMapReduceIndex();
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
            const hasAnyItemSelected = this.gridController() ? this.gridController().getSelectedItems().length > 0 : false;
            const queryResultsAreMatchingDocuments = this.queryResultsContainMatchingDocuments();
            
            return !mapReduce && !hasAnyItemSelected && queryResultsAreMatchingDocuments;
        });
        
        this.canExportToFile = ko.pureComputed(() => {
            const hasNonEmptyResult = this.totalResultsForUi() > 0;

            if (!hasNonEmptyResult) {
                return false;
            }

            const hasAnyItemSelected = this.gridController() ? this.gridController().getSelectedItems().length > 0 : false;
            return !hasAnyItemSelected;
        });
        
        this.deleteDocumentDisableReason = ko.pureComputed(() => {
            const hasAnyItemSelected = this.gridController() ? this.gridController().getSelectedItems().length > 0 : false;
            if (hasAnyItemSelected) {
                return "Please unselect all items before deleting documents";
            }
            
            const canDelete = this.canDeleteDocumentsMatchingQuery();
            if (canDelete) {
                return "";
            } else {
                return "Available only for Map indexes (that are not defined on Counters or Time Series)";
            }
        });
        
        this.showPlotButton = ko.pureComputed(() => {
            const onResultsTab = this.currentTab() === "results";
            if (!onResultsTab) {
                return false;
            }
            return this.gridController() ? this.gridController().getSelectedItems().length > 0 : false;
        });

        this.containsAsterixQuery = ko.pureComputed(() => this.criteria().queryText().includes("*.*"));

        this.staleResult = ko.pureComputed(() => {
            const stats = this.queryStats();
            return stats ? stats.IsStale : false;
        });

        this.cacheEnabled.subscribe(() => {
            eventsCollector.default.reportEvent("query", "toggle-cache");
        });

        this.spinners.isLoading.extend({ rateLimit: 100 });

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

        this.showMapView = ko.pureComputed(() => this.currentTab() instanceof spatialQueryMap);
        
        this.showTimeSeriesGraph = ko.pureComputed(() => this.currentTab() instanceof timeSeriesPlotDetails);
        
        this.showVirtualTable = ko.pureComputed(() => {
            const currentTab = this.currentTab();
            return currentTab !== 'timings'&& currentTab !== "queryPlan" && !this.showTimeSeriesGraph() && !this.showMapView();
        });

        this.spatialResultsOnMapText = ko.pureComputed(() => 
            this.hasMoreSpatialResultsForMap() ? `Showing first ${this.numberOfMarkers().toLocaleString()} results on map.` : "");

        this.numberOfMarkersText = ko.pureComputed(() => {
            const markersNumber = this.numberOfMarkers().toLocaleString();
            return this.hasMoreSpatialResultsForMap() ? markersNumber + "+" : markersNumber;
        });
    }

    private initValidation(): void {
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

    activate(indexNameOrRecentQueryHash?: string, additionalParameters?: { database: string; openGraph: boolean }) {
        super.activate(indexNameOrRecentQueryHash, additionalParameters);
        
        if (additionalParameters && additionalParameters.openGraph) {
            this.autoOpenGraph = true;
        }
        
        this.updateHelpLink('KCIMJK');

        this.disableAutoIndexCreation(activeDatabaseTracker.default.settings().disableAutoIndexCreation.getValue());
        
        const db = this.db;
        
        return this.fetchAllIndexes(db)
            .done(() => this.selectInitialQuery(indexNameOrRecentQueryHash));
    }

    deactivate(): void {
        super.deactivate();

        const currentQueryText = this.criteria().queryText();
        
        if (currentQueryText !== this.lastCriteriaExecuted.queryText()) {
            query.lastQueryNotExecuted.set(this.db.name, currentQueryText);
        }
    }

    attached(): void {
        super.attached();

        this.createKeyboardShortcut("ctrl+enter", () => this.runQuery(), query.ContainerSelector);
        
        this.createKeyboardShortcut("ctrl+s", () => {
            if (!this.inSaveMode()) {
                this.saveQueryToLocalStorage();
                this.saveQueryFocus(true);
            }
        }, query.ContainerSelector);
        
        this.createKeyboardShortcut("enter", () => {
            this.saveQueryToLocalStorage();
        }, query.SaveQuerySelector);

        /* TODO
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+r", () => this.runQuery(), query.containerSelector); // Using keyboard shortcut here, rather than HTML's accesskey, so that we don't steal focus from the editor.
        */

        this.registerDisposableHandler($(window), "storage", () => this.loadSavedQueries());
    }
    
    compositionComplete() {
        super.compositionComplete();

        this.$downloadForm = $("#exportFileForm");
        
        this.setupDisableReasons();

        const grid = this.gridController();

        const documentsProvider = new documentBasedColumnsProvider(this.db, grid, {
            enableInlinePreview: true,
            detectTimeSeries: true,
            customInlinePreview: doc => {
                documentBasedColumnsProvider.showPreview(doc, this.criteria().indexEntries() ? "Index Entry Preview" : null);
            },
            timeSeriesActionHandler: (type, documentId, name, value) => {
                if (type === "plot") {
                    const newChart = new timeSeriesPlotDetails([{ documentId, value, name}]);

                    const existingChart = this.timeSeriesGraphs().find(x => x.isEqual(newChart));
                    if (existingChart) {
                        this.goToTimeSeriesTab(existingChart);
                    } else {
                        this.timeSeriesGraphs.push(newChart);
                        this.initTabTooltip();
                        this.goToTimeSeriesTab(newChart);
                    }
                } else {
                    const newTable = new timeSeriesTableDetails(documentId, name, value);

                    const existingTable = this.timeSeriesTables().find(x => x.isEqual(newTable));
                    if (existingTable) {
                        this.goToTimeSeriesTab(existingTable);
                    } else {
                        this.timeSeriesTables.push(newTable);
                        this.initTabTooltip();
                        this.goToTimeSeriesTab(newTable);
                    }
                }
            }
        });

        const highlightingProvider = new documentBasedColumnsProvider(this.db, grid, {
            enableInlinePreview: false
        });

        if (!this.queryFetcher())
            this.queryFetcher(() => $.when({
                items: [] as document[],
                totalResultCount: 0
            }));
        
        this.explanationsFetcher(() => {
           const allExplanations = this.explanationsCache;
           
           return $.when({
               items: allExplanations.map(x => new document(x)),
               totalResultCount: allExplanations.length
           });
        });

        this.includedRevisionsFetcher(() => {
            const revisionItems = this.includesRevisionsCache().items;

            return $.when({
                items: revisionItems.map(x => new document(x)),
                totalResultCount: revisionItems.length
            });
        });
        
        this.columnsSelector.init(grid,
            (s, t) => this.effectiveFetcher()(s, t),
            (w, r) => {
                const tab = this.currentTab();
                if (tab === "results" || tab instanceof perCollectionIncludes) {
                    return documentsProvider.findColumns(w, r);
                } else if (tab === "explanations") {
                    return this.explanationsColumns(grid);
                } else if (tab === "revisions") {
                    return this.revisionsColumns(grid);
                } else if (tab instanceof timeSeriesTableDetails) {
                    return this.getTimeSeriesColumns(grid, tab);
                } else {
                    return highlightingProvider.findColumns(w, r);
                }
            }, (results: pagedResult<document>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

        grid.headerVisible(true);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.queryFetcher.subscribe(() => grid.reset());
        
        const queryTimingsLink = "https://ravendb.net/l/4FEPMK/" + this.clientVersion();
        const queryTimingsDetails = `<a target="_blank" href="${queryTimingsLink}">Query Timings</a>`;
        
        popoverUtils.longWithHover($(".query-time-info"),
            {
                content: `<small>View timings details by including ${queryTimingsDetails}</small>`,
                html: true
            });

        this.columnPreview.install("virtual-grid", ".js-query-tooltip", 
            (doc: document, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
            if (this.currentTab() === "explanations" && column.header === "Explanation") {
                // we don't want to show inline preview for Explanation column, as value doesn't contain full message
                // which might be misleading - use preview button to obtain entire explanation 
                return;
            }
            
            const showPreview = (value: any) => {
                if (value !== undefined) {
                    const json = JSON.stringify(value, null, 4);
                    const html = highlight(json, languages.javascript, "js");
                    onValue(html, json);
                }
            };

            if (this.currentTab() instanceof timeSeriesTableDetails && column instanceof textColumn) {
                const header = column.header;
                const rawValue = (doc as any)[header];
                const dateHeaders = ["From", "To", "Timestamp"];

                if (_.includes(dateHeaders, header)) {
                    onValue(moment.utc(rawValue).local(), rawValue);
                } else {
                    showPreview(rawValue);
                }
                
                // if value wasn't handled don't fallback to default options
                return;
            }
            
            if (column instanceof textColumn && !(column instanceof timeSeriesColumn)) {
                const value = column.getCellValue(doc);
                showPreview(value);
            }
            
            if (this.currentTab() === "revisions" && column.header === "Last Modified") {
                const rawValue = (doc as any);
                onValue(moment.utc(rawValue.lastModified), rawValue.lastModified);
            }
        });
        
        this.queryHasFocus(true);
        
        this.timingsGraph.syncLegend();
        
        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));
        
        this.criteria().queryText.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }
    
    detached() {
        super.detached();
        
        this.languageService.dispose();
    }

    private initTabTooltip(): void {
        $('.tab-info[data-toggle="tooltip"]').tooltip();
    }
    
    private getQueriedIndexInfo(): void {
        const indexName = this.queriedIndex();
        if (!indexName) {
            this.queriedIndexInfo(null);
        } else {
            const currentIndex = this.indexes() ? this.indexes().find(i => i.Name === indexName) : null;
            if (currentIndex) {
                this.queriedIndexInfo(currentIndex);
            } else {
                if (!indexName.startsWith(queryUtil.DynamicPrefix)) {
                    // fetch indexes since this view may not be up-to-date if index was defined outside of studio
                    this.fetchAllIndexes(this.db)
                        .done(() => {
                            this.queriedIndexInfo(this.indexes() ? this.indexes().find(i => i.Name === indexName) : null);
                        })
                        .fail(() => {
                            this.queriedIndexInfo(null);
                        });
                }
            }
        }
    }
    
    private getTimeSeriesColumns(grid: virtualGridController<any>, tab: timeSeriesTableDetails): virtualColumn[] {
        const valuesCount = timeSeriesQueryResult.detectValuesCount(tab.value);
        const maybeArrayPresenter: (columnName: string) => (dto: timeSeriesQueryGroupedItemResultDto | timeSeriesRawItemResultDto) => string | number
            = valuesCount === 1
            ? (columnName => dto => (dto as any)[columnName][0]?.toLocaleString() ?? "-")
            : (columnName => dto => ((dto as any)[columnName]).map((x: number) => x?.toLocaleString() ?? "-").join("; "));

        const formatTimeSeriesDate = (input: string) => {
            const dateToFormat = moment.utc(input);
            return dateToFormat.format(query.dateTimeFormat) + "Z";
        };
        
        switch (timeSeriesQueryResult.detectResultType(tab.value)) {
            case "grouped": {
                const groupedItems = tab.value.Results as Array<timeSeriesQueryGroupedItemResultDto>;
                const groupKeys = timeSeriesQueryResult.detectGroupKeys(groupedItems);

                const aggregationColumns = groupKeys.map(key => {
                    return new textColumn<timeSeriesQueryGroupedItemResultDto>(grid, maybeArrayPresenter(key), key, (45 / groupKeys.length) + "%");
                });

                return [
                    new textColumn<timeSeriesQueryGroupedItemResultDto>(grid, x => formatTimeSeriesDate(x.From), "From", "15%"),
                    new textColumn<timeSeriesQueryGroupedItemResultDto>(grid, x => formatTimeSeriesDate(x.To), "To", "15%"),
                    new textColumn<timeSeriesQueryGroupedItemResultDto>(grid, x => x.Key, "Key", "15%"),
                    new textColumn<timeSeriesQueryGroupedItemResultDto>(grid, maybeArrayPresenter("Count"), "Count", "10%"),
                    ...aggregationColumns
                ];
            }
            case "raw":
                return [
                    new textColumn<timeSeriesRawItemResultDto>(grid, x => formatTimeSeriesDate(x.Timestamp), "Timestamp", "30%"),
                    new textColumn<timeSeriesRawItemResultDto>(grid, x => x.Tag, "Tag", "30%"),
                    new textColumn<timeSeriesRawItemResultDto>(grid, maybeArrayPresenter("Values"), "Values", "30%"),
                ];
        }
    }
    
    private explanationsColumns(grid: virtualGridController<any>): virtualColumn[] {
        return [
            new actionColumn<explanationItem>(grid, doc => this.showExplanationDetails(doc), "Show", `<i class="icon-preview"></i>`, "72px", {
                title: () => 'Show detailed explanation'
            }),
            new textColumn<explanationItem>(grid, x => x.id, "Id", "30%"),
            new textColumn<explanationItem>(grid, x => x.explanations.map(x => x.split("\n")[0]).join(", "), "Explanation", "50%")
        ];
    }

    private revisionsColumns(grid: virtualGridController<any>): virtualColumn[] {
        return [
            new actionColumn<includedRevisionItem>(grid, x => this.showRevisionDetails(x.revision), "Show", `<i class="icon-preview"></i>`, "72px", {
                    title: () => 'Show revision preview'
                }),
            new textColumn<includedRevisionItem>(grid, x => x.revision, "Revision", "15%"),
            new hyperlinkColumn<includedRevisionItem>(grid, x => x.sourceDocument, x => appUrl.forEditDoc(x.sourceDocument, this.db), "Source Document", "25%", {
                    sortable: "string"
                }),
            new textColumn<includedRevisionItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.lastModified, query.dateTimeFormat), "Last Modified", "25%", {
                    sortable: "string"
                }),
            new textColumn<includedRevisionItem>(grid, x => x.changeVector, "Change Vector", "25%") 
        ];
    }
    
    private showExplanationDetails(details: explanationItem): void {
        app.showBootstrapDialog(new showDataDialog("Explanation for: " + details.id, details.explanations.join("\r\n"), "plain"));
    }

    private showRevisionDetails(revisionDocument: string): void {
        const text = JSON.stringify(revisionDocument, null, 4);
        app.showBootstrapDialog(new showDataDialog("Revision item", text, "javascript"));
    }

    private loadSavedQueries(): void {
        const db = this.db;

        this.savedQueries(savedQueriesStorage.getSavedQueries(db));
        
        const lastQueryThatWasNotExecuted = query.lastQueryNotExecuted.get(db.name);

        if (lastQueryThatWasNotExecuted) {
            this.criteria().queryText(lastQueryThatWasNotExecuted);
            query.lastQueryNotExecuted.set(this.db.name, "");
        }
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getEssentialDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.EssentialDatabaseStatistics) => {
                this.indexes(results.Indexes);
            });
    }

    selectInitialQuery(indexNameOrRecentQueryHash: string): void {
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
                this.navigate(appUrl.forQuery(this.db));
            }
        } else if (indexNameOrRecentQueryHash) {
            messagePublisher.reportError(`Could not find index or recent query: ${indexNameOrRecentQueryHash}`);
            // fallback to All Documents, but show error
            this.runQueryOnIndex(queryUtil.AllDocs);
        }
    }

    runQueryOnIndex(indexName: string): void {
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

        const url = appUrl.forQuery(this.db, indexName);
        this.updateUrl(url);
    }

    killQuery() {
        this.confirmationMessage("Abort the query", "Do you want to abort currently running query?")
            .done(result => {
                if (result.can) {
                    this.showKillQueryButton(false);
                    if (this.spinners.isLoading()) {
                        killQueryCommand.byClientQueryId(this.db, query.clientQueryId)
                            .execute();
                    }
                }
            });
    }
    
    runQuery(optionalSavedQueryName?: string): void {
        if (!this.isValid(this.criteria().validationGroup)) {
            return;
        }
        
        this.allSpatialResultsItems([]);
        this.timeSeriesGraphs([]);
        this.timeSeriesTables([]);
        
        this.columnsSelector.reset();
        
        this.effectiveFetcher = this.queryFetcher;
        this.currentTab("results");
        this.includesCache.removeAll();

        this.clearHighlightsCache();
        this.clearIncludesRevisionsCache();
        this.clearExplanationsCache();
        
        this.timings(null);
        this.showFanOutWarning(false);
        this.queryPlan(null);
        
        this.isEmptyFieldsResult(false);
        
        eventsCollector.default.reportEvent("query", "run");
        const criteria = this.criteria();

        this.saveQueryOptions(criteria);
        
        const criteriaDto = criteria.toStorageDto();
        const disableCache = !this.cacheEnabled();
        const disableAutoIndexCreation = this.disableAutoIndexCreation();
        const projectionBehavior = this.projectionBehavior();

        if (criteria.queryText()) {
            this.spinners.isLoading(true);

            const database = this.db;

            //TODO: this.currentColumnsParams().enabled(this.showFields() === false && this.indexEntries() === false);

            const queryCmd = new queryCommand({
                db: database,
                skip: 0,
                take: 25,
                criteria,
                disableCache,
                disableAutoIndexCreation,
                projectionBehavior
            });

            // we declare this variable here, if any result returns skippedResults <> 0 we enter infinite scroll mode 
            let totalSkippedResults = 0;
            let itemsSoFar = 0;
            
            try {
                this.rawJsonUrl(appUrl.forDatabaseQuery(database) + queryCmd.getUrl("GET"));
            } catch (error) {
                // it may throw when unable to compute query parameters, etc.
                messagePublisher.reportError("Unable to run the query", error.message, null, false);
                this.spinners.isLoading(false);
                return;
            }

            this.lastCriteriaExecuted = criteria.clone();
            
            const resultsFetcher = (skip: number, take: number) => {
                const criteriaForFetcher = this.lastCriteriaExecuted;
                
                const command = new queryCommand({
                    db: database,
                    skip: skip + totalSkippedResults,
                    take: take + 1,
                    criteria: criteriaForFetcher,
                    disableCache,
                    disableAutoIndexCreation,
                    queryId: query.clientQueryId,
                    projectionBehavior
                });

                this.onQueryRun();
                
                const resultsTask = $.Deferred<pagedResultExtended<document>>();
                const queryForAllFields = criteriaForFetcher.showFields();
                
                // Note: 
                // When server response is '304 Not Modified' then the browser cached data contains duration time from the 'first' execution
                // If we ask browser to report the 304 state then 'response content' is empty 
                // This is why we need to measure the execution time here ourselves..
                const startQueryTime = new Date().getTime();
                
                command.execute()
                    .always(() => {
                        this.spinners.isLoading(false);
                        this.afterQueryRun();
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
                        const filterByQuery = !!queryResults.additionalResultInfo.ScannedResults;
                        
                        itemsSoFar += queryResults.items.length;
                        
                        if (totalFromQuery !== -1) {
                            if (itemsSoFar > totalFromQuery) {
                                itemsSoFar = totalFromQuery;
                            }
                        }
                        
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
                        
                        if (queryResults.additionalResultInfo.SkippedResults || filterByQuery) {
                            // apply skipped results (if any)
                            totalSkippedResults += queryResults.additionalResultInfo.SkippedResults;
                            
                            // find if query contains positive offset or limit, if so warn about paging.
                            // eslint-disable-next-line @typescript-eslint/no-unused-vars
                            const [_, rqlWithoutParameters] = queryCommand.extractQueryParameters(criteriaForFetcher.queryText());
                            if (/\s+(offset|limit)\s+/img.test(rqlWithoutParameters)) {
                                this.showFanOutWarning(true);
                            }
                        }
                        
                        if (totalSkippedResults || filterByQuery) {
                            queryResults.totalResultCount = skip + queryResults.items.length;
                            if (queryResults.items.length === take + 1) {
                                queryResults.totalResultCount += 30;
                                const totalWithOffsetAndLimit = queryResults.additionalResultInfo.CappedMaxResults;
                                if (totalWithOffsetAndLimit && totalWithOffsetAndLimit < queryResults.totalResultCount) {
                                    queryResults.totalResultCount = totalWithOffsetAndLimit - 1;
                                }
                                
                                if (totalFromQuery != -1) {
                                    this.hasMoreUnboundedResults(itemsSoFar < totalFromQuery);
                                } else {
                                    this.hasMoreUnboundedResults(true); 
                                }
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
                            && queryResults.items.every(x => x.getDocumentPropertyNames().length === 0);
                        
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
                            if (queryResults.includesRevisions) {
                                this.onIncludesRevisionsLoaded(queryResults.includesRevisions);    
                            }
                            if (queryResults.highlightings) {
                                this.onHighlightingsLoaded(queryResults.highlightings);
                            }
                            
                            if (queryResults.explanations) {
                                this.onExplanationsLoaded(queryResults.explanations, queryResults.items);
                            }
                            this.onTimingsLoaded(queryResults);
                            this.onSpatialLoaded(queryResults);
                        }
                        
                        this.saveRecentQueryToStorage(criteriaDto, optionalSavedQueryName);
                        
                        this.setupDisableReasons(); 
                        
                        if (this.autoOpenGraph) {
                            const firstItem = this.gridController().findItem(() => true);
                            if (firstItem) {
                                this.gridController().setSelectedItems([firstItem]);
                                this.plotTimeSeries();
                            }
                            
                            this.autoOpenGraph = false;
                        }
                        
                        // get index info After query was run because it could be a newly generated auto-index
                        this.getQueriedIndexInfo();
                    })
                    .fail((request: JQueryXHR) => {
                        this.rawJsonUrl(null);
                        this.queryStats(null);
                        this.totalResultsForUi(0);
                        resultsTask.reject(request);
                    });
                
                return resultsTask;
            };

            this.queryFetcher(resultsFetcher);
            this.updateBrowserUrl(this.criteria());
        }
    }
    
    onQueryRun() {
        this.killQueryTimeoutId = setTimeout(() => {
            this.showKillQueryButton(true);
        }, 5000);
    }
    
    afterQueryRun() {
        clearTimeout(this.killQueryTimeoutId);
        this.killQueryTimeoutId = null;
        this.showKillQueryButton(false);
    }
    
    explainIndex(): void {
        new explainQueryCommand(this.criteria().queryText(), this.db)
            .execute()
            .done(explanationResult => {
                app.showBootstrapDialog(new explainQueryDialog(explanationResult));
            });
    }

    saveQueryToLocalStorage(): void {
        if (this.inSaveMode()) {
            eventsCollector.default.reportEvent("query", "save");

            if (this.isValid(this.saveQueryValidationGroup)) {
                
                // Verify if name already exists
                if (savedQueriesStorage.getSavedQueries(this.db).find(x => x.name.toUpperCase() === this.querySaveName().toUpperCase())) {
                    this.confirmationMessage(`Query ${generalUtils.escapeHtml(this.querySaveName())} already exists`, `Overwrite existing query?`, {
                        buttons: ["No", "Overwrite"],
                        html: true
                    })
                        .done(result => {
                            if (result.can) {
                                this.saveQueryToStorage(this.criteria().toStorageDto());
                            }
                        });
                } else {
                    this.saveQueryToStorage(this.criteria().toStorageDto());
                }
                
                this.inSaveMode(false);
            }
        } else {
            if (this.isValid(this.criteria().validationGroup)) {
                this.inSaveMode(true);
            }
        }
    }
    
    private saveQueryOptions(criteria: queryCriteria): void {
        this.queriedFieldsOnly(criteria.showFields());
        this.queriedIndexEntries(criteria.indexEntries());
    }
    
    private saveQueryToStorage(criteria: storedQueryDto): void {
        criteria.name = this.querySaveName();
        this.saveToStorage(criteria, false);
        this.querySaveName(null);
        this.saveQueryValidationGroup.errors.showAllMessages(false);
        messagePublisher.reportSuccess("Query saved successfully");
    }

    private saveRecentQueryToStorage(criteria: storedQueryDto, optionalSavedQueryName?: string): void {
        const name = optionalSavedQueryName || this.getRecentQueryName();
        criteria.name = name;
        this.saveToStorage(criteria, !optionalSavedQueryName);
    }

    private saveToStorage(criteria: storedQueryDto, isRecent: boolean): void {
        criteria.recentQuery = isRecent;
        this.appendQuery(criteria);
        savedQueriesStorage.storeSavedQueries(this.db, this.savedQueries());

        this.criteria().name("");
        this.loadSavedQueries();
    }

    showFirstItemInPreviewArea(): void {
        this.previewItem(savedQueriesStorage.getSavedQueries(this.db)[0]);
    }
    
    appendQuery(criteria: storedQueryDto): void {
        if (criteria.recentQuery) {
            const existing = this.savedQueries().find(query => query.hash === criteria.hash);
            
            if (existing) {
                this.savedQueries.remove(existing);
                this.savedQueries.unshift(criteria);
            } else {
                this.removeLastRecentQueryIfMoreThanLimit();
                this.savedQueries.unshift(criteria);
            }
        } else {
            const existing = this.savedQueries().find(x => x.name === criteria.name);
            
            if (existing) {
                this.savedQueries.remove(existing);
            }

            this.savedQueries.unshift(criteria);
        }
    }

    private removeLastRecentQueryIfMoreThanLimit(): void {
        this.savedQueries()
            .filter(x => x.recentQuery)
            .filter((_, idx) => idx >= query.recentQueryLimit)
            .forEach(x => this.savedQueries.remove(x));
    }

    private getRecentQueryName(): string {
        const [collectionIndexName, type] = queryUtil.getCollectionOrIndexName(this.criteria().queryText());
        return type !== "unknown" ? query.recentKeyword + " (" + collectionIndexName + ")" : query.recentKeyword;
    }

    previewQuery(item: storedQueryDto): void {
        this.previewItem(item);
    }

    useQueryItem(item: storedQueryDto): void {
        this.previewItem(item);
        this.useQuery();
    }

    useQuery(): void {
        const queryDoc = this.criteria();
        const previewItem = this.previewItem();
        queryDoc.copyFrom(previewItem);
        
        // Reset settings
        this.cacheEnabled(true);
        this.criteria().indexEntries(false);
        this.criteria().showFields(false);
        
        this.runQuery(previewItem.recentQuery ? null : previewItem.name);
    }

    removeQuery(item: storedQueryDto): void {
        this.confirmationMessage("Query", `Are you sure you want to delete query '${generalUtils.escapeHtml(item.name)}'?`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {

                    if (this.previewItem() === item) {
                        this.previewItem(null);
                    }

                    savedQueriesStorage.removeSavedQueryByHash(this.db, item.hash);
                    this.loadSavedQueries();
                }
            });
    }
    
    /*
    The rules are:
    - scan through results and pick matching explanations - remove from map after pushing to results
    - if key wasn't found in explanations - ignore and skip
    - iterate through remaining items - push them in any order - we already did our best to sort that
     */
    private onExplanationsLoaded(explanations: dictionary<Array<string>>, results: document[]) {
        const remainingItems = new Map(Object.keys(explanations).map(x => [x, explanations[x]]));
        
        const itemsToCache: explanationItem[] = [];
        results.forEach(result => {
            const id = result.getId();
            const list = remainingItems.get(id);
            if (list) {
                itemsToCache.push({
                    id,
                    explanations: list
                });
                remainingItems.delete(id);
            }
        });

        remainingItems.forEach((value, id) => {
            itemsToCache.push({
                id,
                explanations: value
            });
        });
        
        // scan existing cache for duplicates
        const newIds = new Set<string>(itemsToCache.map(x => x.id));
        this.explanationsCache = this.explanationsCache.filter(x => !newIds.has(x.id));
        
        // and push new cached items
        this.explanationsCache.push(...itemsToCache);
        
        this.totalExplanations(this.explanationsCache.length);
    }
    
    private onTimingsLoaded(queryResults: pagedResultExtended<document>): void { 
        const timings = queryResults.timings;
        this.timings(timings);
        this.queryPlan(queryResults.queryPlan);
    }
    
    private onIncludesLoaded(includes: dictionary<any>): void {
        Object.entries(includes).forEach(([id, doc]) => {
            const metadata = doc["@metadata"];
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

    private onIncludesRevisionsLoaded(includesRevisionsFromQuery: Array<any>): void {
        const mappedItems = includesRevisionsFromQuery.map(x => {
            const metadata = x.Revision["@metadata"];
            const lastModified = metadata ? metadata["@last-modified"] : null;

            return {
                revision: x.Revision,
                lastModified: lastModified,
                sourceDocument: x.Id,
                changeVector: x.ChangeVector
            };
        });

        this.includesRevisionsCache().items.push(...mappedItems);
        this.includesRevisionsCache().total(this.includesRevisionsCache().items.length);
    }
    
    private onHighlightingsLoaded(highlightings: dictionary<dictionary<Array<string>>>): void {
        Object.entries(highlightings).forEach(([fieldName, value]) => {
            let existingPerFieldCache = this.highlightsCache().find(x => x.fieldName() === fieldName);

            if (!existingPerFieldCache) {
                existingPerFieldCache = new highlightSection();
                existingPerFieldCache.fieldName(fieldName);
                this.highlightsCache.push(existingPerFieldCache);
            }
            
            Object.entries(value).forEach(([key, fragments]) => {
               if (!existingPerFieldCache.data.has(key)) {
                   existingPerFieldCache.data.set(key, []);
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
    
    private onSpatialLoaded(queryResults: pagedResultExtended<document>): void {
        this.isSpatialQuery(false);
        
        const spatialProperties = queryResults.additionalResultInfo.SpatialProperties;
        if (spatialProperties && queryResults.items.length) {
            this.isSpatialQuery(true);
            this.allSpatialResultsItems([]);
        }
    }
    
    plotTimeSeries(): void {
        const selection = this.gridController().getSelectedItems();
        
        const timeSeries: timeSeriesPlotItem[] = [];
        
        selection.forEach((item: document) => {
            const documentId = item.getId();
            
            const allColumns = this.columnsSelector.allVisibleColumns();
            
            allColumns.forEach(columnItem => {
                const column = columnItem.virtualColumn();
                if (column instanceof timeSeriesColumn) {
                    timeSeries.push({
                        documentId,
                        name: column.header,
                        value: column.getCellValue(item)
                    });
                }
            });
        });

        const newChart = new timeSeriesPlotDetails(timeSeries);
        
        const existingChart = this.timeSeriesGraphs().find(x => x.isEqual(newChart));
        
        if (existingChart) {
            this.goToTimeSeriesTab(existingChart);
        } else {
            this.timeSeriesGraphs.push(newChart);
            this.initTabTooltip();
            this.goToTimeSeriesTab(newChart);
        }
    }

    refresh(): void {
        this.gridController().reset(true);
    }
    
    openQueryStats(): void {
        //TODO: work on explain in dialog
        eventsCollector.default.reportEvent("query", "show-stats");
        const totalResultsFormatted = this.totalResultsForUi().toLocaleString() + (this.hasMoreUnboundedResults() ? "+" : "");
        const viewModel = new queryStatsDialog(this.queryStats(), totalResultsFormatted, this.db);
        app.showBootstrapDialog(viewModel);
    }

    private updateBrowserUrl(criteria: queryCriteria): void {
        const newQuery: storedQueryDto = criteria.toStorageDto();

        const queryUrl = appUrl.forQuery(this.db, newQuery.hash);
        this.updateUrl(queryUrl);
    }

    runRecentQuery(storedQuery: storedQueryDto): void {
        eventsCollector.default.reportEvent("query", "run-recent");

        const criteria = this.criteria();

        criteria.updateUsing(storedQuery);

        this.runQuery(storedQuery.name);
    }

    getRecentQuerySortText(sorts: string[]): string {
        if (sorts.length > 0) {
            return sorts
                .map(s => querySort.fromQuerySortString(s).toHumanizedString())
                .join(", ");
        }

        return "";
    }

    deleteDocsMatchingQuery(): void {
        eventsCollector.default.reportEvent("query", "delete-documents");

        const db = this.db;
        const viewModel = new deleteDocumentsMatchingQueryConfirm(this.queriedIndex(), this.lastCriteriaExecuted.queryText(), this.totalResultsForUi(), db, this.hasMoreUnboundedResults());

        app.showBootstrapDialog(viewModel)
            .done((result) => {
                if (result) {
                    new deleteDocsMatchingQueryCommand(this.lastCriteriaExecuted.queryText(), this.db)
                        .execute()
                        .done((operationId: operationIdDto) => {
                            this.monitorDeleteOperation(db, operationId.OperationId);
                        });
                }
            });
    }

    syntaxHelp(): void {
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
    
    goToResultsTab(): void {
        this.currentTab("results");
        this.effectiveFetcher = this.queryFetcher;

        // since we merge records based on fragments
        // remove all existing highlights & included revisions when going back to 'results' tab
        
        this.clearHighlightsCache(); 
        this.clearIncludesRevisionsCache();
        this.clearExplanationsCache();
        
        this.columnsSelector.reset();
        this.refresh();
    }
    
    private clearHighlightsCache() {
        this.highlightsCache.removeAll();
    }

    private clearExplanationsCache() {
        this.explanationsCache.length = 0;
        this.totalExplanations(0);
    }
        
    private clearIncludesRevisionsCache() {
        this.includesRevisionsCache(new includedRevisions());
    }
    
    goToIncludesRevisionsTab(): void {
        this.currentTab("revisions");
        
        this.effectiveFetcher = this.includedRevisionsFetcher;

        this.columnsSelector.reset();
        this.refresh();
    }
    
    goToIncludesTab(includes: perCollectionIncludes): void {
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
    
    goToExplanationsTab(): void {
        this.currentTab("explanations");
        this.effectiveFetcher = this.explanationsFetcher;
        
        this.columnsSelector.reset();
        this.refresh();
    }

    goToQueryPlanTab(): void {
        this.resultsExpanded(true);
        this.currentTab("queryPlan");
        this.queryPlanGraph.clearGraph();
        
        setTimeout(() => {
            this.queryPlanGraph.draw(this.queryPlan());    
        }, 200);
    }

    goToHighlightsTab(highlight: highlightSection): void {
        this.currentTab(highlight);

        const itemsFlattened = Array.from(highlight.data.values()).flatMap(items => items);
        
        this.effectiveFetcher = ko.observable<fetcherType>(() => {
            return $.when({
                items: itemsFlattened.map(x => new document(x)),
                totalResultCount: itemsFlattened.length
            });
        });
        this.columnsSelector.reset();
        this.refresh();
    }

    goToTimingsTab(): void {
        this.currentTab("timings");
        
        this.timingsGraph.draw(this.timings());
    }
    
    goToSpatialMapTab(): void {
        if (!this.showMapView()) {
            this.loadMoreSpatialResultsToMap();
        }
    }
    
    loadMoreSpatialResultsToMap(): void {
        this.spinners.isLoadingSpatialResults(true);
        this.failedToGetResultsForSpatial(false);

        const command = new queryCommand({
            db: this.db,
            skip: this.allSpatialResultsItems().length,
            take: query.maxSpatialResultsToFetch + 1,
            criteria: this.criteria().clone(),
            disableCache: !this.cacheEnabled(),
            disableAutoIndexCreation: this.disableAutoIndexCreation()
        });
        
        command.execute()
            .done((queryResults: pagedResultExtended<document>) => {
                const spatialProperties = queryResults.additionalResultInfo.SpatialProperties;
                this.populateSpatialMap(queryResults, spatialProperties);
                this.currentTab(this.spatialMap());
            })
            .fail(() => this.failedToGetResultsForSpatial(true))
            .always(() => this.spinners.isLoadingSpatialResults(false));
    }

    private populateSpatialMap(queryResults: pagedResultExtended<document>, spatialProperties: any): void {
        // Each spatial markers model will contain the layer of markers per spatial properties pair 
        const spatialMarkersLayers: spatialMarkersLayerModel[] = [];
        const spatialCirclesLayer: spatialCircleModel[] = [];
        const spatialPolygonsLayer: spatialPolygonModel[] = [];

        let markersCount = 0;
        this.hasMoreSpatialResultsForMap(false);

        if (queryResults.items.length === query.maxSpatialResultsToFetch + 1) {
            queryResults.items.length--;
            this.hasMoreSpatialResultsForMap(true);
        }

        this.allSpatialResultsItems.push(...queryResults.items);

        for (let i = 0; i < spatialProperties.length; i++) {
            const latitudeProperty = spatialProperties[i].LatitudeProperty;
            const longitudeProperty = spatialProperties[i].LongitudeProperty;

            const pointsArray: geoPointInfo[] = [];
            for (let i = 0; i < this.allSpatialResultsItems().length; i++) {
                const item = this.allSpatialResultsItems()[i];

                const latitudeValue = _.get(item, latitudeProperty) as number;
                const longitudeValue = _.get(item, longitudeProperty) as number;

                if (latitudeValue != null && longitudeValue != null) {
                    const point: geoPointInfo = { Latitude: latitudeValue, Longitude: longitudeValue, PopupContent: item };
                    pointsArray.push(point);
                    markersCount++;
                }
            }

            const layer = new spatialMarkersLayerModel(latitudeProperty, longitudeProperty, pointsArray);
            spatialMarkersLayers.push(layer);
        }

        this.numberOfMarkers(markersCount);

        const spatialShapes = queryResults.additionalResultInfo.SpatialShapes;
        for (let i = 0; i < spatialShapes.length; i++) {
            const shape = spatialShapes[i];
            switch (shape.Type) {
                case "Circle": {
                    const circle = new spatialCircleModel(shape as Raven.Server.Documents.Indexes.Spatial.Circle);
                    spatialCirclesLayer.push(circle);
                }
                    break;
                case "Polygon": {
                    const polygon = new spatialPolygonModel(shape as Raven.Server.Documents.Indexes.Spatial.Polygon);
                    spatialPolygonsLayer.push(polygon);
                }
                    break;
            }
        }
        
        const spatialMapView = new spatialQueryMap(spatialMarkersLayers, spatialCirclesLayer, spatialPolygonsLayer);
        this.spatialMap(spatialMapView);
    }

    goToTimeSeriesTab(tab: timeSeriesPlotDetails | timeSeriesTableDetails): void {
        this.currentTab(tab);
        this.resultsExpanded(true);

        if (tab instanceof timeSeriesTableDetails) {
            this.effectiveFetcher = ko.observable<fetcherType>(() => {
                return $.when({
                    items: tab.value.Results.map(x => new document(x)),
                    totalResultCount: tab.value.Results.length
                });
            });

            this.columnsSelector.reset();
            this.refresh();
        }
    }

    closeTimeSeriesTab(tab: timeSeriesPlotDetails | timeSeriesTableDetails): void {
        if (this.currentTab() === tab) {
            this.goToResultsTab();
        }

        if (tab instanceof timeSeriesPlotDetails) {
            this.timeSeriesGraphs.remove(tab);
        }

        if (tab instanceof timeSeriesTableDetails) {
            this.timeSeriesTables.remove(tab);
        }
    }

    toggleResults(): void {
        this.resultsExpanded.toggle();
        this.gridController().reset(true);
        if (this.currentTab() === this.spatialMap()) {
            this.spatialMap().onResize();
            this.allSpatialResultsItems([]);
            this.loadMoreSpatialResultsToMap();
        }
    }
    
    exportAsFile() {
        eventsCollector.default.reportEvent("query", "export-csv");

        const args = {
            format: this.exportAsFileSettings.format(),
            field: this.exportAsFileSettings.allColumns() ? undefined : this.columnsSelector.getSimpleColumnsFields(),
            debug: this.criteria().indexEntries() ? "entries" : undefined,
            includeLimit: this.criteria().ignoreIndexQueryLimit() ? "true" : undefined
        }

        let payload: { Query: string, QueryParameters: string };
        const [queryParameters, rqlWithoutParameters] = queryCommand.extractQueryParameters(this.criteria().queryText());

        if (this.criteria().showFields()) {
            payload = {
                Query: queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(rqlWithoutParameters),
                QueryParameters: queryParameters
            };
        } else {
            payload = {
                Query: rqlWithoutParameters,
                QueryParameters: queryParameters
            };
        }
        $("input[name=ExportOptions]").val(JSON.stringify(payload));

        const url = appUrl.forDatabaseQuery(this.db) + endpoints.databases.streaming.streamsQueries + appUrl.urlEncodeArgs(args);
        this.$downloadForm.attr("action", url);
        this.$downloadForm.submit();
    }

    setProjectionBehavior(projectionBehavior: Raven.Client.Documents.Queries.ProjectionBehavior) {
        this.projectionBehavior(projectionBehavior);
        // Force dropdown menu to close. (for nested dropdowns, the menu stays open)
        $(".projection-behavior-dropdown").removeClass("open");
    }
    
    formatTime(input: number) {
        if (!input) {
            return "<1 ms";
        }
        
        return input.toLocaleString() + " ms";
    }
}

export = query;
