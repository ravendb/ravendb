import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import queryIndexCommand = require("commands/queryIndexCommand");
import moment = require("moment");
import deleteIndexesConfirm = require("viewmodels/deleteIndexesConfirm");
import querySort = require("models/querySort");
import getTransformersCommand = require("commands/getTransformersCommand");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/deleteDocumentsMatchingQueryConfirm");

class query extends viewModelBase {

    selectedIndex = ko.observable<string>();
    indexNames = ko.observableArray<string>();
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

    static containerSelector = "#queryContainer";

    constructor() {
        super();

        this.editIndexUrl = ko.computed(() => this.selectedIndex() ? appUrl.forEditIndex(this.selectedIndex(), this.activeDatabase()) : null);
        this.termsUrl = ko.computed(() => this.selectedIndex() ? appUrl.forTerms(this.selectedIndex(), this.activeDatabase()) : null);
        this.statsUrl = ko.computed(() => appUrl.forStatus(this.activeDatabase()));
        this.hasSelectedIndex = ko.computed(() => this.selectedIndex() != null);
        this.selectedIndexEditUrl = ko.computed(() => this.selectedIndex() ? appUrl.forEditIndex(this.selectedIndex(), this.activeDatabase()) : '');
        
        aceEditorBindingHandler.install();        
    }

    activate(indexToSelect?: string) {
        super.activate(indexToSelect);

        this.fetchAllIndexes(indexToSelect);
        this.fetchAllTransformers();
    }

    attached() {
        this.useBootstrapTooltips();
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        $("#indexQueryLabel").popover({
            html: true,
            trigger: 'hover',
            container: '#indexQueryLabelContainer',
            content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
        });
    }

    deactivate() {
        super.deactivate();
        this.removeKeyboardShortcuts(query.containerSelector);
    }

    editSelectedIndex() {
        router.navigate(this.editIndexUrl());
    }

    fetchAllIndexes(indexToSelect?: string) {
        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => {
                this.indexNames(stats.Indexes.map(i => i.PublicName));
                this.setSelectedIndex(indexToSelect || this.indexNames().first());
            });
    }

    fetchAllTransformers() {
        new getTransformersCommand(this.activeDatabase())
            .execute()
            .done((results: transformerDto[]) => this.allTransformers(results));
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
            var useAndOperator = this.isDefaultOperatorOr() === false;
            var resultsFetcher = (skip: number, take: number) => {
                var command = new queryIndexCommand(selectedIndex, database, skip, take, queryText, sorts, transformer, showFields, indexEntries, useAndOperator);
                return command
                    .execute()
                    .done((queryResults: pagedResultSet) => this.queryStats(queryResults.additionalResultInfo));
            };
            var resultsList = new pagedList(resultsFetcher);
            this.queryResults(resultsList);

            return resultsList;
        }

        return null;
    }

    setSelectedIndex(indexName: string) {
        this.sortBys.removeAll();
        this.selectedIndex(indexName);
        this.runQuery();

        // Fetch the index definition so that we get an updated list of fields.
        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((result: indexDefinitionContainerDto) => {
                this.indexFields(result.Index.Fields);
            });

        // Reflect the new index in the address bar.
        var url = appUrl.forQuery(this.activeDatabase(), indexName);
        var navOptions: DurandalNavigationOptions = {
            replace: true,
            trigger: false
        };
        router.navigate(url, navOptions);
        NProgress.done();
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
}

export = query;