import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getCustomSortersCommand = require("commands/database/settings/getCustomSortersCommand");
import deleteCustomSorterCommand = require("commands/database/settings/deleteCustomSorterCommand");
import database = require("models/resources/database");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import documentObject = require("models/database/documents/document");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getServerWideCustomSortersCommand = require("commands/serverWide/sorters/getServerWideCustomSortersCommand");
import queryCommand = require("commands/database/query/queryCommand");
import queryCriteria = require("models/database/query/queryCriteria");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import sorterListItemModel = require("models/database/settings/sorterListItemModel");
import accessManager = require("common/shell/accessManager");
import rqlLanguageService = require("common/rqlLanguageService");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import getIndexNamesCommand from "commands/database/index/getIndexNamesCommand";
import shardedDatabase from "models/resources/shardedDatabase";
import shard from "models/resources/shard";

type testTabName = "results" | "diagnostics";
type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

class customSorters extends shardViewModelBase {

    view = require("views/database/settings/customSorters.html");

    indexes = ko.observableArray<string>();
    languageService: rqlLanguageService;
    
    canUseCustomSorters: boolean;
    
    sorters = ko.observableArray<sorterListItemModel>([]);
    serverWideSorters = ko.observableArray<sorterListItemModel>([]);
    
    addUrl = ko.pureComputed(() => appUrl.forEditCustomSorter(this.db));

    serverWideCustomSortersUrl = appUrl.forServerWideCustomSorters();
    canNavigateToServerWideCustomSorters: KnockoutComputed<boolean>;

    private gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector<documentObject>();

    syntaxCheckSubscription: KnockoutSubscription;
    
    currentTab = ko.observable<testTabName>("results");
    effectiveFetcher = ko.observable<fetcherType>();
    resultsFetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();
    
    isFirstRun = true;

    diagnostics = ko.observableArray<string>([]);
    resultsCount = ko.observable<number>(0);
    diagnosticsCount = ko.observable<number>(0);
    
    testResultsVisible = ko.observable<boolean>(false);

    clientVersion = viewModelBase.clientVersion;
    localNodeTag = clusterTopologyManager.default.localNodeTag();

    constructor(db: database) {
        super(db);
        
        this.canUseCustomSorters = !(db instanceof shardedDatabase) && !(db instanceof shard);

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("confirmRemoveSorter", "enterTestSorterMode", "editSorter", "runTest");

        this.canNavigateToServerWideCustomSorters = accessManager.default.isClusterAdminOrClusterNode;
        
        this.languageService = new rqlLanguageService(this.db, this.indexes, "Select");
    }
    
    activate(args: any) {
        super.activate(args);
        
        if (!this.canUseCustomSorters) {
            return ;
        }
        
        return $.when<any>(this.loadSorters(), this.loadServerWideSorters(), this.fetchIndexNames(this.db)
            .done(() => { 
                const serverWideSorterNames = this.serverWideSorters().map(x => x.name);
                
                this.sorters().forEach(sorter => {
                    if (_.includes(serverWideSorterNames, sorter.name)) {
                        sorter.overrideServerWide(true);
                    }
                })
            }));
    }

    compositionComplete() {
        super.compositionComplete();

        $('.custom-sorters [data-toggle="tooltip"]').tooltip();
    }
    
    detached() {
        super.detached();
        
        this.languageService.dispose();
    }

    private fetchIndexNames(db: database): JQueryPromise<any> {
        return new getIndexNamesCommand(db)
            .execute()
            .done((results: string[]) => {
                this.indexes(results);
            });
    }
    
    private loadSorters() {
        return new getCustomSortersCommand(this.db)
            .execute()
            .done(sorters => {
                this.sorters(sorters.map(x => new sorterListItemModel(x)));
            });
    }

    private loadServerWideSorters() {
        return new getServerWideCustomSortersCommand()
            .execute()
            .done(sorters => this.serverWideSorters(sorters.map(x => new sorterListItemModel(x))));
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
    
    enterTestSorterMode(sorter: sorterListItemModel) {
        this.closeTestResultsArea();
        
        sorter.testModeEnabled.toggle();
        
        if (sorter.testModeEnabled()) {
            // close other sections
            this.sorters()
                .filter(x => x !== sorter)
                .forEach(x => x.testModeEnabled(false));
        }

        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));
        
        if (this.syntaxCheckSubscription) {
            this.syntaxCheckSubscription.dispose();
            this.syntaxCheckSubscription = null;
        }
        
        this.syntaxCheckSubscription = sorter.testRql.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }
    
    editSorter(sorter: sorterListItemModel) {
        const url = appUrl.forEditCustomSorter(this.db, sorter.name);
        router.navigate(url);
    }
    
    confirmRemoveSorter(sorter: sorterListItemModel) {
        this.confirmationMessage("Delete Custom Sorter", 
            `You're deleting custom sorter: <br><ul><li><strong>${generalUtils.escapeHtml(sorter.name)}</strong></li></ul>`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    this.sorters.remove(sorter);
                    this.deleteSorter(this.db, sorter.name);
                }
            })
    }
    
    runTest(sorter: sorterListItemModel) {
        this.testResultsVisible(true);
        
        this.currentTab("results");

        this.columnsSelector.reset();
        
        const criteria = queryCriteria.empty();
        criteria.queryText(sorter.testRql());
        criteria.diagnostics(true);
        
        const queryTask = $.Deferred<pagedResult<documentObject>>();
        
        new queryCommand(this.db, 0, 128, criteria)
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
        const documentsProvider = new documentBasedColumnsProvider(this.db, this.gridController(), {
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
                            const html = highlight(json, languages.javascript, "js");
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
