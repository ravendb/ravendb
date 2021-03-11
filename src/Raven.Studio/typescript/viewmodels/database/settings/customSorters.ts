import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getCustomSortersCommand = require("commands/database/settings/getCustomSortersCommand");
import deleteCustomSorterCommand = require("commands/database/settings/deleteCustomSorterCommand");
import database = require("models/resources/database");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import documentObject = require("models/database/documents/document");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import queryCompleter = require("common/queryCompleter");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import queryCommand = require("commands/database/query/queryCommand");
import queryCriteria = require("models/database/query/queryCriteria");

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");

class sorterListItem {
    
    definition: Raven.Client.Documents.Queries.Sorting.SorterDefinition;
    
    testModeEnabled = ko.observable<boolean>(false);
    testRql = ko.observable<string>();
    
    constructor(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        this.definition = dto;
        this.testRql(`from index <indexName>\r\norder by custom(<fieldName>, "${dto.Name}")`);
    }
    
    get name() {
        return this.definition.Name;
    }
}

type testTabName = "results" | "diagnostics";
type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

class customSorters extends viewModelBase {

    indexes = ko.observableArray<Raven.Client.Documents.Operations.IndexInformation>();
    queryCompleter = queryCompleter.remoteCompleter(this.activeDatabase, this.indexes, "Select");
    
    sorters = ko.observableArray<sorterListItem>([]);
    
    addUrl = ko.pureComputed(() => appUrl.forEditCustomSorter(this.activeDatabase()));

    private gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector<documentObject>();
    
    currentTab = ko.observable<testTabName>("results");
    effectiveFetcher = ko.observable<fetcherType>();
    resultsFetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();
    
    isFirstRun = true;

    diagnostics = ko.observableArray<string>([]);
    resultsCount = ko.observable<number>(0);
    diagnosticsCount = ko.observable<number>(0);
    
    testResultsVisible = ko.observable<boolean>(false);
    
    constructor() {
        super();

        aceEditorBindingHandler.install();
        
        this.bindToCurrentInstance("confirmRemoveSorter", "enterTestSorterMode", "editSorter", "runTest");
    }
    
    activate(args: any) {
        super.activate(args);
        
        return $.when<any>(this.loadSorters(), this.fetchAllIndexes(this.activeDatabase()));
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.indexes(results.Indexes);
            });
    }
    
    private loadSorters() {
        return new getCustomSortersCommand(this.activeDatabase())
            .execute()
            .done(sorters => {
                this.sorters(sorters.map(x => new sorterListItem(x)));
            });
    }

    goToTab(tabToUse: testTabName) {
        this.currentTab(tabToUse);

        if (tabToUse === "results") {
            this.effectiveFetcher(this.resultsFetcher());
        } else {
            this.effectiveFetcher(() => {
                return $.when({
                    items: this.diagnostics().map(d => new documentObject({
                        Message: d
                    })),
                    totalResultCount: this.diagnosticsCount()
                });
            });
        }
        
        this.columnsSelector.reset();
        this.gridController().reset(true);
    }
    
    closeTestResultsArea() {
        this.testResultsVisible(false);
    }
    
    enterTestSorterMode(sorter: sorterListItem) {
        this.closeTestResultsArea();
        
        sorter.testModeEnabled.toggle();
        
        if (sorter.testModeEnabled()) {
            // close other sections
            this.sorters()
                .filter(x => x !== sorter)
                .forEach(x => x.testModeEnabled(false));
        }
    }
    
    editSorter(sorter: sorterListItem) {
        const url = appUrl.forEditCustomSorter(this.activeDatabase(), sorter.name);
        router.navigate(url);
    }
    
    confirmRemoveSorter(sorter: sorterListItem) {
        this.confirmationMessage("Delete Custom Sorter", 
            `You're deleting custom sorter:  <br><ul><li>${generalUtils.escapeHtml(sorter.name)}</li></ul>`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    this.sorters.remove(sorter);
                    this.deleteSorter(this.activeDatabase(), sorter.name);
                }
            })
    }
    
    runTest(sorter: sorterListItem) {
        this.testResultsVisible(true);
        
        this.currentTab("results");

        this.columnsSelector.reset();
        
        const criteria = queryCriteria.empty();
        criteria.queryText(sorter.testRql());
        criteria.diagnostics(true);
        
        const queryTask = $.Deferred<pagedResult<documentObject>>();
        
        new queryCommand(this.activeDatabase(), 0, 128, criteria)
            .execute()
            .done(results => {
                this.resultsCount(results.items.length);
                this.diagnosticsCount(results.additionalResultInfo.Diagnostics.length);
                this.diagnostics(results.additionalResultInfo.Diagnostics);
                
                const mappedResult = {
                    items: results.items,
                    totalResultCount: results.items.length
                };
                
                queryTask.resolve(mappedResult);
            })
            .fail(response => queryTask.reject(response));
        
        this.resultsFetcher(() => queryTask);

        this.effectiveFetcher(this.resultsFetcher());
        
        if (this.isFirstRun) {
            this.initGrid();
        }
    }
    
    private initGrid() {
        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.gridController(), {
            showRowSelectionCheckbox: false,
            showSelectAllCheckbox: false,
            enableInlinePreview: true
        });

        this.columnsSelector.init(this.gridController(),
            (s, t, c) => this.effectiveFetcher()(s, t),
            (w, r) => {
                if (this.currentTab() === "results") {
                    return documentsProvider.findColumns(w, r, ["__metadata"]);
                } else {
                    return [
                        new textColumn<documentObject>(grid, (x: any) => x.Message, "Message", "100%")
                    ]
                }
            },
            (results: pagedResult<documentObject>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

        const grid = this.gridController();

        grid.headerVisible(true);

        this.columnPreview.install("virtual-grid", ".custom-sorters-tooltip",
            (doc: documentObject, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy: string) => void) => {
                if (column instanceof textColumn) {
                    const value = column.getCellValue(doc);
                    if (!_.isUndefined(value)) {
                        if (column.header === "Exception" && _.isString(value)) {
                            const formattedValue = _.replace(value, "\r\n", "<Br />");
                            onValue(formattedValue, value);
                        } else {
                            const json = JSON.stringify(value, null, 4);
                            const html = Prism.highlight(json, (Prism.languages as any).javascript);
                            onValue(html, json);
                        }
                    }
                }
            });
        
        this.effectiveFetcher.subscribe(() => {
            this.columnsSelector.reset();
            grid.reset();
        });

        this.isFirstRun = false;
    }
    
    private deleteSorter(db: database, name: string) {
        return new deleteCustomSorterCommand(db, name)
            .execute()
            .always(() => {
                this.loadSorters();
            })
    }

}

export = customSorters;
