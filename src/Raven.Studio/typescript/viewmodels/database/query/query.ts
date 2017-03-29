import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats"); 
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/database/query/deleteDocumentsMatchingQueryConfirm");
import deleteDocsMatchingQueryCommand = require("commands/database/documents/deleteDocsMatchingQueryCommand");
import notificationCenter = require("common/notifications/notificationCenter");

import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import database = require("models/resources/database");
import querySort = require("models/database/query/querySort");
import collection = require("models/database/documents/collection");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import document = require("models/database/documents/document");
import queryStatsDialog = require("viewmodels/database/query/queryStatsDialog");
import transformerType = require("models/database/index/transformer");
import recentQueriesStorage = require("common/storage/recentQueriesStorage");
import queryUtil = require("common/queryUtil");
import eventsCollector = require("common/eventsCollector");
import queryCriteria = require("models/database/query/queryCriteria");
import queryTransformerParameter = require("models/database/query/queryTransformerParameter");

import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import popoverUtils = require("common/popoverUtils");

type indexItem = {
    name: string;
    isMapReduce: boolean;
}

type filterType = "in" | "string" | "range";

type stringSearchType = "Starts With" | "Ends With" | "Contains" | "Exact";

type rangeSearchType = "Numeric Double" | "Numeric Long" | "Alphabetical" | "Datetime";

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<document>>;

class query extends viewModelBase {

    static readonly autoPrefix = "auto/";

    // TODO: use a static dynamic prefix with the slash. Note: the index name of 'dynamic/All Documents is actually 'dynamic'
    //static readonly dynamicPrefix = "dynamic/"; 

    static readonly ContainerSelector = "#queryContainer";
    static readonly $body = $("body");

    static readonly SearchTypes: stringSearchType[] = ["Starts With", "Ends With", "Contains", "Exact"];
    static readonly RangeSearchTypes: rangeSearchType[] = ["Numeric Double", "Numeric Long", "Alphabetical", "Datetime"];
    static readonly SortTypes: querySortType[] = ["Ascending", "Descending", "Range Ascending", "Range Descending"];

    private gridController = ko.observable<virtualGridController<any>>();

    recentQueries = ko.observableArray<storedQueryDto>();
    allTransformers = ko.observableArray<Raven.Client.Documents.Transformers.TransformerDefinition>();

    collections = ko.observableArray<collection>([]);
    indexes = ko.observableArray<indexItem>();
    indexFields = ko.observableArray<string>();
    querySummary = ko.observable<string>();

    criteria = ko.observable<queryCriteria>(queryCriteria.empty());
    disableCache = ko.observable<boolean>(false);

    filterSettings = {
        searchField: ko.observable<string>(),
        type: ko.observable<filterType>(),
        value: ko.observable<string>(),
        searchType: ko.observable<stringSearchType>(),
        rangeFrom: ko.observable<string>(),
        rangeTo: ko.observable<string>(),
        rangeDateFrom: ko.observable<moment.Moment>(),
        rangeDateTo: ko.observable<moment.Moment>(),
        rangeSearchType: ko.observable<rangeSearchType>(),
        inValues: ko.observable<string>(),
        validationGroup: null as KnockoutValidationGroup,
    }

    isDateFilter: KnockoutComputed<boolean>;
    isRangeFilter: KnockoutComputed<boolean>;
    isInFilter: KnockoutComputed<boolean>;
    isStringFilter: KnockoutComputed<boolean>;

    columnsSelector = new columnsSelector<document>();

    uiTransformer = ko.observable<string>(); // represents UI value, which might not be yet applied to criteria 
    uiTransformerParameters = ko.observableArray<queryTransformerParameter>(); // represents UI value, which might not be yet applied to criteria 

    fetcher = ko.observable<fetcherType>();
    queryStats = ko.observable<Raven.Client.Documents.Queries.QueryResult<any>>();
    requestedIndexForQuery = ko.observable<string>();
    staleResult: KnockoutComputed<boolean>;
    dirtyResult = ko.observable<boolean>();

    private columnPreview = new columnPreviewPlugin<document>();

    selectedIndexLabel: KnockoutComputed<string>;
    hasEditableIndex: KnockoutComputed<boolean>;
    queryTextHasFocus = ko.observable<boolean>(false);
    addFilterVisible = ko.observable<boolean>(false);
    addFilterEnabled: KnockoutComputed<boolean>;

    editIndexUrl: KnockoutComputed<string>;
    indexPerformanceUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    visualizerUrl: KnockoutComputed<string>;
    rawJsonUrl = ko.observable<string>();
    csvUrl = ko.observable<string>();

    isIndexMapReduce: KnockoutComputed<boolean>;
    isAutoIndex = ko.observable<boolean>(false);
    isStaticIndexSelected: KnockoutComputed<boolean>;
    isLoading = ko.observable<boolean>(false);
    containsAsterixQuery: KnockoutComputed<boolean>; // query contains: *.* ?
    

    /*TODO
    isTestIndex = ko.observable<boolean>(false);
    
    selectedResultIndices = ko.observableArray<number>();
    
    isDynamicIndex: KnockoutComputed<boolean>;
    
    enableDeleteButton: KnockoutComputed<boolean>;
    warningText = ko.observable<string>();
    isWarning = ko.observable<boolean>(false);
    
    contextName = ko.observable<string>();

    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());

    indexSuggestions = ko.observableArray<indexSuggestion>([]);
    showSuggestions: KnockoutComputed<boolean>;

    static queryGridSelector = "#queryResultsGrid";*/

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        aceEditorBindingHandler.install();
        datePickerBindingHandler.install();

        this.initObservables();
        this.initValidation();
        this.resetFilterSettings();

        this.bindToCurrentInstance("runRecentQuery", "selectTransformer", "addFilter", "removeSortBy");
    }

    private initObservables() {
        this.selectedIndexLabel = ko.pureComputed(() => {
            const criteria = this.criteria();

            return (!criteria || criteria.selectedIndex() === "dynamic") ? "All Documents" : criteria.selectedIndex();
        });

        this.hasEditableIndex = ko.pureComputed(() => this.criteria().selectedIndex() ? !this.criteria().selectedIndex().startsWith("dynamic") : false);

        this.addFilterEnabled = ko.pureComputed(() => this.indexFields().length > 0);

        this.editIndexUrl = ko.pureComputed(() => this.criteria().selectedIndex() ? appUrl.forEditIndex(this.criteria().selectedIndex(), this.activeDatabase()) : null);
        this.indexPerformanceUrl = ko.pureComputed(() => this.criteria().selectedIndex() ? appUrl.forIndexPerformance(this.activeDatabase(), this.criteria().selectedIndex()) : null);
        this.termsUrl = ko.pureComputed(() => this.criteria().selectedIndex() ? appUrl.forTerms(this.criteria().selectedIndex(), this.activeDatabase()) : null);
        this.visualizerUrl = ko.pureComputed(() => this.criteria().selectedIndex() ? appUrl.forVisualizer(this.activeDatabase(), this.criteria().selectedIndex()) : null);
        this.isIndexMapReduce = ko.computed(() => {
            const currentIndex = this.indexes().find(i => i.name === this.criteria().selectedIndex());
            return !!currentIndex && currentIndex.isMapReduce;
        });
        this.isStaticIndexSelected = ko.pureComputed(() => !this.criteria().selectedIndex().startsWith("dynamic"));

        this.containsAsterixQuery = ko.pureComputed(() => this.criteria().queryText().includes("*.*"));

        this.isDateFilter = ko.pureComputed(() => this.filterSettings.type() === "range" && this.filterSettings.rangeSearchType() === "Datetime");
        this.isInFilter = ko.pureComputed(() => this.filterSettings.type() === "in");
        this.isStringFilter = ko.pureComputed(() => this.filterSettings.type() === "string");
        this.isRangeFilter = ko.pureComputed(() => this.filterSettings.type() === "range");
        

        const dateToString = (input: moment.Moment) => input ? input.format("YYYY-MM-DDTHH:mm:00.0000000") : "";

        this.filterSettings.rangeDateFrom.subscribe(() => this.filterSettings.rangeFrom(dateToString(this.filterSettings.rangeDateFrom())));
        this.filterSettings.rangeDateTo.subscribe(() => this.filterSettings.rangeTo(dateToString(this.filterSettings.rangeDateTo())));

        this.staleResult = ko.pureComputed(() => {
            //TODO: return false for test index
            const stats = this.queryStats();
            return stats ? stats.IsStale : false;
        });

        this.disableCache.subscribe(() => {
            eventsCollector.default.reportEvent("query", "toggle-cache");
        });

        this.addFilterVisible.subscribe(visible => {
            query.$body.toggleClass('show-add-filter', visible);
        });

        this.queryTextHasFocus.subscribe(v => {
            if (v) {
                this.addFilterVisible(true);
            }
        });

        this.rawJsonUrl.subscribe((value: string) => ko.postbox.publish("SetRawJSONUrl", value));

        this.isLoading.extend({ rateLimit: 100 });

        const criteria = this.criteria();

        criteria.showFields.subscribe(() => this.runQuery());
        criteria.indexEntries.subscribe(() => this.runQuery());
        criteria.useAndOperator.subscribe(() => this.runQuery());

         /* TODO
        this.showSuggestions = ko.computed<boolean>(() => {
            return this.indexSuggestions().length > 0;
        });

        this.selectedIndex.subscribe(index => this.onIndexChanged(index));

        this.isDynamicIndex = ko.computed(() => {
            var currentIndex = this.indexes().find(i => i.name === this.selectedIndex());
            return !!currentIndex && currentIndex.name.startsWith("Auto/");
        });

        this.enableDeleteButton = ko.computed(() => {
            var currentIndex = this.indexes().find(i => i.name === this.selectedIndex());
            var isMapReduce = this.isIndexMapReduce();
            var isDynamic = this.isDynamicIndex();
            return !!currentIndex && !isMapReduce && !isDynamic;
        });*/
    }

    private initValidation() {
        const filter = this.filterSettings;

        filter.searchField.extend({
            required: true
        });

        filter.rangeFrom.extend({
            number: {
                onlyIf: () => this.isRangeFilter() && filter.rangeSearchType().startsWith("Numeric")
            }
        });

        filter.rangeTo.extend({
            number: {
                onlyIf: () => this.isRangeFilter() && filter.rangeSearchType().startsWith("Numeric")
            }
        });

        filter.type.subscribe(() => {
            // clean all messages after changing type
            filter.validationGroup.errors.showAllMessages(false);
        });

        filter.value.extend({
            required: {
                onlyIf: () => filter.type() === "string"
            }
        });

        filter.inValues.extend({
            required: {
                onlyIf: () => filter.type() === 'in'
            }
        });

        filter.validationGroup = ko.validatedObservable({
            searchField: filter.searchField,
            value: filter.value,
            inValues: filter.inValues,
            rangeFrom: filter.rangeFrom,
            rangeTo: filter.rangeTo
        });
    }

    canActivate(args: any) {
        super.canActivate(args);

        this.loadRecentQueries();

        //TODO: fetch custom functions and use $.when

        const initTask = $.Deferred<canActivateResultDto>();

        this.fetchAllTransformers(this.activeDatabase())
            .done(() => initTask.resolve({ can: true }))
            .fail(() => initTask.resolve({ can: false }));

        return initTask;
    }

    activate(indexNameOrRecentQueryHash?: string) {
        super.activate(indexNameOrRecentQueryHash);

        this.updateHelpLink('KCIMJK');
        
        const db = this.activeDatabase();

        return $.when<any>(this.fetchAllCollections(db), this.fetchAllIndexes(db))
            .done(() => this.selectInitialQuery(indexNameOrRecentQueryHash));
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    attached() {
        super.attached();

        this.createKeyboardShortcut("ctrl+enter", () => this.runQuery(), query.ContainerSelector);

        /* TODO
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+r", () => this.runQuery(), query.containerSelector); // Using keyboard shortcut here, rather than HTML's accesskey, so that we don't steal focus from the editor.
        */

        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexQueryRawData(this.activeDatabase(), this.criteria().selectedIndex()));

        $(".query-title small").popover({
            html: true,
            trigger: "hover",
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            content: '<p>Queries use Lucene syntax. Examples:</p><pre><span class="token keyword">Name</span>: Hi?berna*<br/><span class="token keyword">Count</span>: [0 TO 10]<br/><span class="token keyword">Title</span>: "RavenDb Queries 1010" <span class="token keyword">AND Price</span>: [10.99 TO *]</pre>'
        });

        this.registerDisposableHandler($(window), "storage", () => this.loadRecentQueries());
    }

    compositionComplete() {
        super.compositionComplete();

        this.setupDisableReasons();

        const grid = this.gridController();

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.collections().map(x => x.name), {
            enableInlinePreview: true
        });

        this.columnsSelector.init(grid,
            (s, t, c) => this.fetcher()(s, t),
            (w, r) => documentsProvider.findColumns(w, r), (results: pagedResult<document>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

        grid.headerVisible(true);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.fetcher.subscribe(() => grid.reset());

        this.columnPreview.install("virtual-grid", ".tooltip", (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column instanceof textColumn) {
                const value = column.getCellValue(doc);
                if (!_.isUndefined(value)) {
                    const json = JSON.stringify(value, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html);
                }
            }
        });
    }

    private loadRecentQueries() {
        recentQueriesStorage.getRecentQueriesWithIndexNameCheck(this.activeDatabase())
            .done(queries => this.recentQueries(queries));
    }

    private fetchAllTransformers(db: database): JQueryPromise<Array<Raven.Client.Documents.Transformers.TransformerDefinition>> {
        return new getTransformersCommand(db)
            .execute()
            .done(transformers => this.allTransformers(transformers));
    }

    private fetchAllCollections(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db)
            .execute()
            .done((results: collectionsStats) => {
                this.collections(results.collections);
            });
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.indexes(results.Indexes.map(indexDto => {
                    return {
                        name: indexDto.Name,
                        isMapReduce: indexDto.Type === "MapReduce" //TODO: support for autoindexes?
                    } as indexItem;
                }));
            });
    }

    selectInitialQuery(indexNameOrRecentQueryHash: string) {
        if (!indexNameOrRecentQueryHash) {
            // if no index exists ==> use the default dynamic/All Documents
            this.setSelectedIndex("dynamic");
        } else if (this.indexes().find(i => i.name === indexNameOrRecentQueryHash) ||
            indexNameOrRecentQueryHash.startsWith("dynamic")) {
            this.setSelectedIndex(indexNameOrRecentQueryHash);
        } else if (indexNameOrRecentQueryHash.indexOf("recentquery-") === 0) {
            const hash = parseInt(indexNameOrRecentQueryHash.substr("recentquery-".length), 10);
            const matchingQuery = this.recentQueries().find(q => q.hash === hash);
            if (matchingQuery) {
                this.runRecentQuery(matchingQuery);
            } else {
                this.navigate(appUrl.forQuery(this.activeDatabase()));
            }
        } else if (indexNameOrRecentQueryHash) {
            messagePublisher.reportError("Could not find " + indexNameOrRecentQueryHash + " index");
            // fallback to all documents, but show error
            this.setSelectedIndex("dynamic");
        }
    }

    setSelectedIndex(indexName: string) {
        this.isAutoIndex(indexName.toLowerCase().startsWith(query.autoPrefix));
        this.criteria().setSelectedIndex(indexName);
        this.resetFilterSettings();
        this.uiTransformer(null);
        this.uiTransformerParameters([]);

        this.columnsSelector.reset();    
        
        this.runQuery();

        const indexQuery = query.getIndexUrlPartFromIndexName(indexName);
        const url = appUrl.forQuery(this.activeDatabase(), indexQuery);
        this.updateUrl(url);

        queryUtil.fetchIndexFields(this.activeDatabase(), indexName, this.indexFields);
    }

    private resetFilterSettings() {
        this.filterSettings.searchField(undefined);
        this.filterSettings.type("string");
        this.filterSettings.value(undefined);
        this.filterSettings.searchType("Starts With");
        this.filterSettings.rangeFrom(undefined);
        this.filterSettings.rangeTo(undefined);
        this.filterSettings.rangeDateFrom(undefined);
        this.filterSettings.rangeDateTo(undefined);
        this.filterSettings.rangeSearchType("Numeric Double");
        this.filterSettings.inValues(undefined);
        this.filterSettings.validationGroup.errors.showAllMessages(false);
    }

    static getIndexUrlPartFromIndexName(indexNameOrCollectionName: string) {
        if (indexNameOrCollectionName === "All Documents") {
            return "dynamic";
        }

        return indexNameOrCollectionName;
    }

    private generateQuerySummary() {
        const criteria = this.criteria();
        const sorts = criteria.sorts().filter(x => x.fieldName());
        const transformer = criteria.transformer();

        const sortPart = sorts.length ? "sorted by " + sorts.map(x => x.toHumanizedString()).join(", ") : "";
        const transformerPart = transformer ? "transformed by " + transformer : "";

        return sortPart + (sortPart && transformerPart ? " and " : "") + transformerPart;
    }

    runQuery() {
        eventsCollector.default.reportEvent("query", "run");
        this.queryTextHasFocus(false);
        this.closeAddFilter();
        this.querySummary(this.generateQuerySummary());
        const criteria = this.criteria();
        const selectedIndex = criteria.selectedIndex();
        this.requestedIndexForQuery(selectedIndex);
        if (selectedIndex) {
            //TODO: this.isWarning(false);
            this.isLoading(true);

            const database = this.activeDatabase();

            //TODO: this.currentColumnsParams().enabled(this.showFields() === false && this.indexEntries() === false);

            const queryCommand = new queryIndexCommand(database, 0, 25, this.criteria(), this.disableCache());

            this.rawJsonUrl(appUrl.forDatabaseQuery(database) + queryCommand.getUrl());
            this.csvUrl(queryCommand.getCsvUrl());

            const resultsFetcher = (skip: number, take: number) => {
                const command = new queryIndexCommand(database, skip, take, this.criteria(), this.disableCache());
                return command.execute()
                    .always(() => {
                        this.isLoading(false);
                    })
                    .done((queryResults: pagedResult<any>) => {
                        this.queryStats(queryResults.additionalResultInfo);
                        //TODO: this.indexSuggestions([]);
                        /* TODO
                        if (queryResults.totalResultCount == 0) {
                            var queryFields = this.extractQueryFields();
                            var alreadyLookedForNull = false;
                            if (this.selectedIndex().indexOf(this.dynamicPrefix) !== 0) {
                                alreadyLookedForNull = true;
                                for (var i = 0; i < queryFields.length; i++) {
                                    this.getIndexSuggestions(selectedIndex, queryFields[i]);
                                    if (queryFields[i].FieldValue == 'null') {
                                        this.isWarning(true);
                                        this.warningText(<any>("The Query contains '" + queryFields[i].FieldName + ": null', this will check if the field contains the string 'null', is this what you meant?"));
                                    }
                                }
                            }
                            if (!alreadyLookedForNull) {
                                for (var i = 0; i < queryFields.length; i++) {
                                    ;
                                    if (queryFields[i].FieldValue == 'null') {
                                        this.isWarning(true);
                                        this.warningText(<any>("The Query contains '" + queryFields[i].FieldName + ": null', this will check if the field contains the string 'null', is this what you meant?"));
                                    }
                                }
                            }
                        }*/
                    })
                    .fail((request: JQueryXHR) => {
                        if (request.status === 404) {
                            recentQueriesStorage.removeIndexFromRecentQueries(database, selectedIndex);
                        }
                    });
            };

            this.fetcher(resultsFetcher);
            this.recordQueryRun(this.criteria());
        }
    }

    refresh() {
        this.gridController().reset(true);
    }
    
    editSelectedIndex() {
        eventsCollector.default.reportEvent("query", "edit-selected-index");
        this.navigate(this.editIndexUrl());
    }

    closeAddFilter() {
        this.addFilterVisible(false);
    }

    openAddFilter() {
        this.addFilterVisible(true);
    }

    openQueryStats() {
        //TODO: work on explain in dialog + on index name
        eventsCollector.default.reportEvent("query", "show-stats");
        const viewModel = new queryStatsDialog(this.queryStats(), this.requestedIndexForQuery());
        app.showBootstrapDialog(viewModel);
    }

    private recordQueryRun(criteria: queryCriteria) {
        const newQuery: storedQueryDto = criteria.toStorageDto();

        const queryUrl = appUrl.forQuery(this.activeDatabase(), newQuery.hash);
        this.updateUrl(queryUrl);

        recentQueriesStorage.appendQuery(newQuery, this.recentQueries);
        recentQueriesStorage.saveRecentQueries(this.activeDatabase(), this.recentQueries());
    }

    runRecentQuery(storedQuery: storedQueryDto) {
        eventsCollector.default.reportEvent("query", "run-recent");

        const criteria = this.criteria();

        criteria.updateUsing(storedQuery);
        criteria.sorts().forEach(sort => sort.bindOnUpdateAction(() => this.runQuery()));

        const matchedTransformer = this.allTransformers().find(t => t.Name === criteria.transformer());

        if (matchedTransformer) {
            this.selectTransformer(matchedTransformer);
            this.fillTransformerParameters(criteria.transformerParameters());
        } else {
            this.selectTransformer(null);
        }

        this.runQuery();
    }

    private fillTransformerParameters(transformerParameters: Array<transformerParamDto>) {
        transformerParameters.forEach(param => {
            const matchingField = this.uiTransformerParameters().find(x => x.name === param.name);
            if (matchingField) {
                matchingField.value(param.value);
            }
        });
    }

    selectTransformer(transformer: Raven.Client.Documents.Transformers.TransformerDefinition) {
        if (transformer) {
            this.uiTransformer(transformer.Name);
            const inputs = transformerType.extractInputs(transformer.TransformResults);
            this.uiTransformerParameters(inputs.map(input => new queryTransformerParameter(input)));
        } else {
            this.uiTransformer(null);
            this.uiTransformerParameters([]);
        }
    }

    private validateTransformer(): boolean {
        if (!this.uiTransformer() || this.uiTransformerParameters().length === 0) {
            return true;
        }

        let valid = true;

        this.uiTransformerParameters().forEach(param => {
            if (!this.isValid(param.validationGroup)) {
                valid = false;
            }
        });

        return valid;
    }

    applyTransformer() {
        if (this.validateTransformer()) {

            $("transform-results-btn").dropdown("toggle");

            const criteria = this.criteria();
            const transformerToApply = this.uiTransformer();
            if (transformerToApply) {
                criteria.transformer(transformerToApply);
                criteria.transformerParameters(this.uiTransformerParameters().map(p => p.toDto()));
                this.runQuery();
            } else {
                criteria.transformer(null);
                criteria.transformerParameters([]);
                this.runQuery();
            }
        }
    }

    getStoredQueryTransformerParameters(queryParams: Array<transformerParamDto>): string {
        if (queryParams.length > 0) {
            return "(" +
                queryParams
                    .map((param: transformerParamDto) => param.name + "=" + param.value)
                    .join(", ") + ")";
        }

        return "";
    }

    addFilter() {
        const filter = this.filterSettings;
        if (this.isValid(filter.validationGroup)) {

            switch (filter.type()) {
                case 'string':
                    this.addStringFilter();
                    break;
                case 'in':
                    this.addInFilter();
                    break;
                case 'range':
                    this.addRangeFilter();
                    break;
            }
        }
    }

    private addStringFilter() {
        const filter = this.filterSettings;
        eventsCollector.default.reportEvent("query", "field-name-starts-with");

        const escapedTerm = queryUtil.escapeTerm(filter.value());
        let newQueryPart: string = "";

        if (this.criteria().queryText().trim().length > 0) {
            newQueryPart += " AND ";
        }
        const field = filter.searchField();

        switch (filter.searchType()) {
            case 'Starts With':
                newQueryPart += field + ":" + escapedTerm + "*";
                break;
            case "Ends With":
                newQueryPart += field + ":*" + escapedTerm;
                break;
            case "Contains":
                newQueryPart += field + ":*" + escapedTerm + "*";
                break;
            case "Exact":
                newQueryPart += field + ":" + escapedTerm;
                break;
        }

        this.criteria().queryText(this.criteria().queryText() + newQueryPart);
    }

    private addInFilter() {
        const filter = this.filterSettings;
        eventsCollector.default.reportEvent("query", "field-value-in");

        const values = filter.inValues().split(",");
        const escapedValues = values.map(x => queryUtil.escapeTerm(x));
        const field = filter.searchField();

        let newQueryPart: string = "";

        if (this.criteria().queryText().trim().length > 0) {
            newQueryPart += " AND ";
        }

        newQueryPart += " @in<" + field + ">:(" + escapedValues.join() + ")";
        this.criteria().queryText(this.criteria().queryText() + newQueryPart);
    }

    private addRangeFilter() {
        const filter = this.filterSettings;
        eventsCollector.default.reportEvent("query", "field-value-range");

        const from = filter.rangeFrom() || "*";
        const to = filter.rangeTo() || "*";

        let newQueryPart: string = "";

        if (this.criteria().queryText().trim().length > 0) {
            newQueryPart += " AND ";
        }

        const rangePrefix = this.getRangePrefix(filter.rangeSearchType());

        newQueryPart += filter.searchField() + rangePrefix + ":[" + from + " TO " + to + "]";

        this.criteria().queryText(this.criteria().queryText() + newQueryPart);

    }

    private getRangePrefix(type: rangeSearchType): string {
        if (type === "Numeric Double") {
            return "_D_Range";
        } else if (type === "Numeric Long") {
            return "_L_Range";
        }
        return "";
    }

    addSortBy() {
        eventsCollector.default.reportEvent("query", "add-sort-by");

        const newSort = querySort.empty();
        newSort.bindOnUpdateAction(() => this.runQuery());

        this.criteria().sorts.push(newSort);
    }

    removeSortBy(sortBy: querySort) {
        eventsCollector.default.reportEvent("query", "remove-sort-by");

        this.criteria().sorts.remove(sortBy);
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

    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        queryUtil.queryCompleter(this.indexFields, this.criteria().selectedIndex, this.activeDatabase, editor, session, pos, prefix, callback);
    }

    deleteDocsMatchingQuery() {
        eventsCollector.default.reportEvent("query", "delete-documents");
        // Run the query so that we have an idea of what we'll be deleting.
        this.runQuery();
        this.fetcher()(0, 1)
            .done((results) => {
                if (results.totalResultCount === 0) {
                    app.showBootstrapMessage("There are no documents matching your query.", "Nothing to do");
                } else {
                    const usedIndex = this.queryStats().IndexName; // used to handle deletes on dynamic collections
                    this.promptDeleteDocsMatchingQuery(results.totalResultCount, usedIndex);
                }
            });
    }

    private promptDeleteDocsMatchingQuery(resultCount: number, index: string) {
        const criteria = this.criteria();

        const db = this.activeDatabase();
        const viewModel = new deleteDocumentsMatchingQueryConfirm(criteria.selectedIndex(), criteria.queryText(), resultCount, db);
        app
            .showBootstrapDialog(viewModel)
            .done((result) => {
                if (result) {
                    new deleteDocsMatchingQueryCommand(index,
                            criteria.queryText(),
                            this.activeDatabase())
                        .execute()
                        .done((operationId: operationIdDto) => {
                            this.monitorDeleteOperation(db, operationId.OperationId);
                        });
                }
            });
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

    /* TODO
    extractQueryFields(): Array<queryFieldInfo> {
        var query = this.queryText();
        var luceneSimpleFieldRegex = /(\w+):\s*("((?:[^"\\]|\\.)*)"|'((?:[^'\\]|\\.)*)'|(\w+))/g;

        var queryFields: Array<queryFieldInfo> = [];
        var match: RegExpExecArray = null;
        while ((match = luceneSimpleFieldRegex.exec(query))) {
            var value = match[3] || match[4] || match[5];
            queryFields.push({
                FieldName: match[1],
                FieldValue: value,
                Index: match.index
            });
        }
        return queryFields;
    }
   
*/

    /* TODO future:

     getIndexSuggestions(indexName: string, info: queryFieldInfo) {
        if (_.includes(this.indexFields(), info.FieldName)) {
            var task = new getIndexSuggestionsCommand(this.activeDatabase(), indexName, info.FieldName, info.FieldValue).execute();
            task.done((result: suggestionsDto) => {
                for (var index = 0; index < result.Suggestions.length; index++) {
                    this.indexSuggestions.push({
                        Index: info.Index,
                        FieldName: info.FieldName,
                        FieldValue: info.FieldValue,
                        Suggestion: result.Suggestions[index]
                    });
                }
            });
        }
    }

    applySuggestion(suggestion: indexSuggestion) {
        eventsCollector.default.reportEvent("query", "apply-suggestion");
        var value = this.queryText();
        var startIndex = value.indexOf(suggestion.FieldValue, suggestion.Index);
        this.queryText(value.substring(0, startIndex) + suggestion.Suggestion + value.substring(startIndex + suggestion.FieldValue.length));
        this.indexSuggestions([]);
        this.runQuery();
    }

      exportCsv() {
        eventsCollector.default.reportEvent("query", "export-csv");

        var db = this.activeDatabase();
        var url = appUrl.forDatabaseQuery(db) + this.csvUrl();
        this.downloader.download(db, url);
    }

     onIndexChanged(newIndexName: string) {
        var command = getCustomColumnsCommand.forIndex(newIndexName, this.activeDatabase());
        this.contextName(command.docName);

        command.execute().done((dto: customColumnsDto) => {
            if (dto) {
                this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                this.currentColumnsParams().customMode(true);
            } else {
                // use default values!
                this.currentColumnsParams().columns.removeAll();
                this.currentColumnsParams().customMode(false);
            }

        });
    }
    */

}
export = query;