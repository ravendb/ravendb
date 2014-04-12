import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import queryIndexCommand = require("commands/queryIndexCommand");
import moment = require("moment");
import deleteIndexesConfirm = require("viewmodels/deleteIndexesConfirm");
import querySort = require("models/querySort");
import collection = require("models/collection");
import getTransformersCommand = require("commands/getTransformersCommand");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/deleteDocumentsMatchingQueryConfirm");
import getStoredQueriesCommand = require("commands/getStoredQueriesCommand");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import document = require("models/document");
import customColumnParams = require('models/customColumnParams');
import customColumns = require('models/customColumns');
import selectColumns = require('viewmodels/selectColumns');
import getCustomColumnsCommand = require('commands/getCustomColumnsCommand');


class query extends viewModelBase {

    selectedIndex = ko.observable<string>();
    indexes = ko.observableArray<{name:string; hasReduce:boolean}>();
    editIndexUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    statsUrl: KnockoutComputed<string>;
    hasSelectedIndex: KnockoutComputed<boolean>;
    queryText = ko.observable("");
    queryResults = ko.observable<pagedList>();
    selectedResultIndices = ko.observableArray<number>();
    queryStats = ko.observable<indexQueryResultsDto>();
    selectedIndexEditUrl: KnockoutComputed<string>;
    sortBys = ko.observableArray<querySort>();
    indexFields = ko.observableArray<string>();
    transformer = ko.observable<string>();
    allTransformers = ko.observableArray<transformerDto>();
    isDefaultOperatorOr = ko.observable(true);
    showFields = ko.observable(false);
    indexEntries = ko.observable(false);
    recentQueries = ko.observableArray<storedQueryDto>();
    recentQueriesDoc = ko.observable<storedQueryContainerDto>();
    rawJsonUrl = ko.observable<string>();
    collectionNames = ko.observableArray<string>();
    selectedIndexLabel: KnockoutComputed<string>;
    appUrls: computedAppUrls;
    isIndexMapReduce = ko.computed(() => {
        var currentIndex = this.indexes.first(i=> i.name == this.selectedIndex());
        return !!currentIndex && currentIndex.hasReduce == true;
    });
    contextName = ko.observable<string>();
    didDynamicChangeIndex: KnockoutComputed<boolean>;

    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());

    static containerSelector = "#queryContainer";

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
        this.editIndexUrl = ko.computed(() => this.selectedIndex() ? appUrl.forEditIndex(this.selectedIndex(), this.activeDatabase()) : null);
        this.termsUrl = ko.computed(() => this.selectedIndex() ? appUrl.forTerms(this.selectedIndex(), this.activeDatabase()) : null);
        this.statsUrl = ko.computed(() => appUrl.forStatus(this.activeDatabase()));
        this.hasSelectedIndex = ko.computed(() => this.selectedIndex() != null);
        this.rawJsonUrl.subscribe((value: string) => ko.postbox.publish("SetRawJSONUrl", value));
        this.selectedIndexLabel = ko.computed(() => this.selectedIndex() === "dynamic" ? "All Documents" : this.selectedIndex());
        this.selectedIndexEditUrl = ko.computed(() => {
            if (this.queryStats()){
                var index = this.queryStats().IndexName;
                if (index && index.indexOf("dynamic/") !== 0) {
                    return appUrl.forEditIndex(index, this.activeDatabase());
                }
            }

            return "";
        });

        this.didDynamicChangeIndex = ko.computed(() => {
            if (this.queryStats()) {
                var recievedIndex = this.queryStats().IndexName;
                var selectedIndex = this.selectedIndex();
                return selectedIndex.indexOf("dynamic/") === 0 && this.indexes()[0].name !== recievedIndex;
            } else {
                return false;
            }
        });
        
        aceEditorBindingHandler.install();        
    }

    activate(indexNameOrRecentQueryHash?: string) {
        super.activate(indexNameOrRecentQueryHash);

        this.fetchAllTransformers();
        $.when(
            this.fetchAllCollections(),
            this.fetchAllIndexes(),
            this.fetchRecentQueries()
            ).done(() => this.selectInitialQuery(indexNameOrRecentQueryHash));

        this.selectedIndex.subscribe(index => this.onIndexChanged(index));
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

    selectInitialQuery(indexNameOrRecentQueryHash: string) {
        if (!indexNameOrRecentQueryHash && this.indexes().length > 0) {
            this.setSelectedIndex(this.indexes.first().name);
        } else if (this.indexes.first( i => i.name == indexNameOrRecentQueryHash) || indexNameOrRecentQueryHash.indexOf("dynamic/") === 0 || indexNameOrRecentQueryHash === "dynamic") {
            this.setSelectedIndex(indexNameOrRecentQueryHash);
        }
        else if (indexNameOrRecentQueryHash.indexOf("recentquery-") === 0) {
            var hash = parseInt(indexNameOrRecentQueryHash.substr("recentquery-".length), 10);
            var matchingQuery = this.recentQueries.first(q => q.Hash === hash);
            if (matchingQuery) {
                this.runRecentQuery(matchingQuery);
            }
        }
    }

    attached() {
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        this.createKeyboardShortcut("ctrl+enter", () => this.runQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnQuery(), query.containerSelector);
        $("#indexQueryLabel").popover({
            html: true,
            trigger: 'hover',
            container: '.form-horizontal',
            content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
        });        
        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexQueryRawData(this.activeDatabase(),this.selectedIndex()));
    }

    focusOnQuery() {
        var editorElement = $("#queryEditor").length == 1 ? $("#queryEditor")[0] : null;
        if (editorElement) {
            var docEditor = ko.utils.domData.get($("#queryEditor")[0], "aceEditor");
            if (docEditor) {
                docEditor.focus();
            }
        }
    }

    editSelectedIndex() {
        this.navigate(this.editIndexUrl());
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => this.indexes(results.Indexes.map(i=> {
            return {
                name: i.PublicName,
                hasReduce: !!i.LastReducedTimestamp
            };
        })));
    }

    fetchAllCollections(): JQueryPromise<any> {
        return new getCollectionsCommand(this.activeDatabase())
            .execute()
            .done((results: collection[]) => this.collectionNames(results.map(c => c.name)));
    }

    fetchRecentQueries(): JQueryPromise<any> {
        var result = $.Deferred();
        var storedQueriesCommand = new getStoredQueriesCommand(this.activeDatabase());
        storedQueriesCommand.execute()
            .fail(_ => {
                var newStoredQueryContainer: storedQueryContainerDto = {
                    '@metadata': {},
                    Queries: []
                };
                this.recentQueriesDoc(newStoredQueryContainer);
                this.recentQueries(newStoredQueryContainer.Queries);
            })
            .done((doc: document) => {
                var dto = <storedQueryContainerDto>doc.toDto(true);
                this.recentQueriesDoc(dto);
                this.recentQueries(dto.Queries);
            })
            .always(() => result.resolve());

        return result;
    }

    fetchAllTransformers() {
        new getTransformersCommand(this.activeDatabase())
            .execute()
            .done((results: transformerDto[]) => this.allTransformers(results));
    }

    runRecentQuery(query: storedQueryDto) {
        this.selectedIndex(query.IndexName);
        this.queryText(query.QueryText);
        this.showFields(query.ShowFields);
        this.indexEntries(query.IndexEntries);
        this.isDefaultOperatorOr(query.UseAndOperator === false);
        this.transformer(query.TransformerName);
        this.sortBys(query.Sorts.map(s => querySort.fromQuerySortString(s)));
        this.runQuery();
    }

    runQuery(): pagedList {
        var selectedIndex = this.selectedIndex();
        if (selectedIndex) {
            var queryText = this.queryText();
            var sorts = this.sortBys().filter(s => s.fieldName() != null);
            var database = this.activeDatabase();
            var transformer = this.transformer();
            var showFields = this.showFields();
            var indexEntries = this.indexEntries();

            this.currentColumnsParams().enabled(this.showFields() === false && this.indexEntries() === false);

            var useAndOperator = this.isDefaultOperatorOr() === false;
            this.rawJsonUrl(appUrl.forResourceQuery(this.activeDatabase()) + new queryIndexCommand(selectedIndex, database, 0, 1024, queryText, sorts, transformer, showFields, indexEntries, useAndOperator).getUrl());
            var resultsFetcher = (skip: number, take: number) => {
                var command = new queryIndexCommand(selectedIndex, database, skip, take, queryText, sorts, transformer, showFields, indexEntries, useAndOperator);
                return command
                    .execute()
                    .done((queryResults: pagedResultSet) => this.queryStats(queryResults.additionalResultInfo));
            };
            var resultsList = new pagedList(resultsFetcher);
            this.queryResults(resultsList);
            this.recordQueryRun(selectedIndex, queryText, sorts.map(s => s.toQuerySortString()), transformer, showFields, indexEntries, useAndOperator);

            return resultsList;
        }

        return null;
    }

    recordQueryRun(indexName: string, queryText: string, sorts: string[], transformer: string, showFields: boolean, indexEntries: boolean, useAndOperator: boolean) {
        var newQuery: storedQueryDto = {
            IndexEntries: indexEntries,
            IndexName: indexName,
            IsPinned: false,
            QueryText: queryText,
            ShowFields: showFields,
            Sorts: sorts,
            TransformerName: transformer || null,
            UseAndOperator: useAndOperator,
            Hash: (indexName + (queryText || "") + sorts.reduce((a, b) => a + b, "") + (transformer || "") + showFields + indexEntries + useAndOperator).hashCode()
        };

        // Put the query into the URL, so that if the user refreshes the page, he's still got this query loaded.
        var queryUrl = appUrl.forQuery(this.activeDatabase(), newQuery.Hash);
        this.updateUrl(queryUrl);

        // Add this query to our recent queries list in the UI, or move it to the top of the list if it's already there.
        var existing = this.recentQueries.first(q => q.Hash === newQuery.Hash);
        if (existing) {
            this.recentQueries.remove(existing);
            this.recentQueries.unshift(existing);
        } else {
            this.recentQueries.unshift(newQuery);
        }

        // Limit us to 15 query recent runs.
        if (this.recentQueries().length > 15) {
            this.recentQueries.remove(this.recentQueries()[15]);
        }

        var recentQueriesDoc = this.recentQueriesDoc();
        if (recentQueriesDoc) {
            recentQueriesDoc.Queries = this.recentQueries();
            var preppedDoc = new document(recentQueriesDoc);
            new saveDocumentCommand(getStoredQueriesCommand.storedQueryDocId, preppedDoc, this.activeDatabase(), false)
                .execute()
                .done((result: { Key: string; ETag: string; }) => recentQueriesDoc['@metadata']['@etag'] = result.ETag);
        }
    }

    getRecentQuerySortsText(recentQueryIndex: number) {
        var sorts = this.recentQueries()[recentQueryIndex].Sorts;
        if (sorts.length === 0) {
            return "";
        }
        return sorts
            .map(s => querySort.fromQuerySortString(s))
            .map(s => s.toHumanizedString())
            .reduce((first, second) => first + ", " + second);
    }

    setSelectedIndex(indexName: string) {
        this.sortBys.removeAll();
        this.selectedIndex(indexName);
        this.runQuery();

        // Reflect the new index in the address bar.
        var indexQuery = query.getIndexUrlPartFromIndexName(indexName);
        var url = appUrl.forQuery(this.activeDatabase(), indexQuery);
        this.updateUrl(url);
        NProgress.done();

        // Fetch the index definition so that we get an updated list of fields to be used as sort by options.
        // Fields don't show for All Documents.
        var isAllDocumentsDynamicQuery = indexName === "All Documents";
        if (!isAllDocumentsDynamicQuery) {
            new getIndexDefinitionCommand(indexQuery, this.activeDatabase())
                .execute()
                .done((result: indexDefinitionContainerDto) => {
                    this.indexFields(result.Index.Fields);
                });
        }
    }

    static getIndexUrlPartFromIndexName(indexNameOrCollectionName: string) {
        if (indexNameOrCollectionName === "All Documents") {
            return "dynamic";
        }

        return indexNameOrCollectionName;
    }

    addSortBy() {
        var sort = new querySort();
        sort.fieldName.subscribe(() => this.runQuery());
        sort.sortDirection.subscribe(() => this.runQuery());
        this.sortBys.push(sort);
    }

    removeSortBy(sortBy: querySort) {
        this.sortBys.remove(sortBy);
        this.runQuery();
    }

    addTransformer() {
        this.transformer("");
    }

    selectTransformer(transformer: transformerDto) {
        this.transformer(transformer.name);
        this.runQuery();
    }

    removeTransformer() {
        this.transformer(null);
        this.runQuery();
    }

    setOperatorOr() {
        this.isDefaultOperatorOr(true);
        this.runQuery();
    }

    setOperatorAnd() {
        this.isDefaultOperatorOr(false);
        this.runQuery();
    }

    toggleShowFields() {
        this.showFields(!this.showFields());
        this.runQuery();
    }

    toggleIndexEntries() {
        this.indexEntries(!this.indexEntries());
        this.runQuery();
    }

    deleteDocsMatchingQuery() {
        // Run the query so that we have an idea of what we'll be deleting.
        var queryResult = this.runQuery();
        queryResult
            .fetch(0, 1)
            .done((results: pagedResultSet) => {
                if (results.totalResultCount === 0) {
                    app.showMessage("There are no documents matching your query.", "Nothing to do");
                } else {
                    this.promptDeleteDocsMatchingQuery(results.totalResultCount);
                }
            });
    }

    promptDeleteDocsMatchingQuery(resultCount: number) {
        var viewModel = new deleteDocumentsMatchingQueryConfirm(this.selectedIndex(), this.queryText(), resultCount, this.activeDatabase());
        app
            .showDialog(viewModel)
            .done(() => this.runQuery());
    }

    selectColumns() {
        var selectColumnsViewModel: selectColumns = new selectColumns(this.currentColumnsParams().clone(), this.contextName(), this.activeDatabase());
        app.showDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols: customColumns) => {
            this.currentColumnsParams(cols);

            this.runQuery();
            
        });
    }

}

export = query;