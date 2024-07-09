import viewModelBase = require("viewmodels/viewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import awesomeMultiselect = require("common/awesomeMultiselect");
import runningQueriesWebSocketClient = require("common/runningQueriesWebSocketClient");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import databasesManager = require("common/shell/databasesManager");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import killQueryCommand = require("commands/database/query/killQueryCommand");
import messagePublisher = require("common/messagePublisher");
import generalUtils = require("common/generalUtils");
import { highlight, languages } from "prismjs";

type ExecutingQueryInfoWithCache = {
    DatabaseName: string;
    IndexName: string;
    RunningQuery: Raven.Server.Documents.Queries.ExecutingQueryInfo;
}

class runningQueries extends viewModelBase {

    view = require("views/manage/runningQueries.html");
    
    filter = ko.observable<string>();
    
    private data = ko.observableArray<ExecutingQueryInfoWithCache>([]);
    private filteredData = ko.observableArray<ExecutingQueryInfoWithCache>([]);

    private gridController = ko.observable<virtualGridController<ExecutingQueryInfoWithCache>>();
    private columnPreview = new columnPreviewPlugin<ExecutingQueryInfoWithCache>();
    
    tailEnabled = ko.observable<boolean>(true);
    private liveClient: runningQueriesWebSocketClient;
    
    private allDbNames = ko.observableArray<string>();
    private selectedDbNames = ko.observableArray<string>();

    toggleTail() {
        this.tailEnabled.toggle();
        
        if (this.tailEnabled()) {
            // clear the table to avoid stale data when ws is connecting
            this.onData([]);
            
            this.connectToWebSocket();
        } else {
            this.disconnectWebSocket();
        }
    }
    
    activate(args: { database: string }) {
        super.activate(args);
        
        this.allDbNames(databasesManager.default.databases().map(x => x.name));
        this.selectedDbNames(args && args.database ? [args.database] : this.allDbNames().slice(0));

        const onCriteriaChanged = () => {
            this.filterData();
            this.gridController().reset(false);
        };

        this.filter
            .throttle(500)
            .subscribe(() => onCriteriaChanged());

        this.selectedDbNames.throttle(500)
            .subscribe(() => onCriteriaChanged());
    }

    deactivate() {
        super.deactivate();

        this.disconnectWebSocket();
    }

    attached() {
        super.attached();
        awesomeMultiselect.build($("#visibleDbsSelector"), opts => {
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " databased selected";
            opts.allSelectedText = "All databases selected";
        });
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(3, "desc");
        grid.init(() => this.fetchData(), () =>
            [
                new actionColumn<ExecutingQueryInfoWithCache>(grid, x => this.killQuery(x), "Abort", `<i class="icon-force"></i>`, "70px",
                    {
                        title: () => 'Abort this query'
                    }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => x.DatabaseName, "Database Name", "15%", {
                    sortable: "string"
                }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => x.IndexName, "Index Name", "20%", {
                    sortable: "string"
                }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => x.RunningQuery.DurationInMs.toLocaleString() + " ms", "Duration", "10%", {
                    sortable: x => x.RunningQuery.DurationInMs
                }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => x.RunningQuery.IsStreaming, "Streaming", "10%", {
                    sortable: "string"
                }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => (x.RunningQuery.QueryInfo as Raven.Client.Documents.Queries.IndexQuery<any>).Query, "Query", "30%", {
                    sortable: "string"
                }),
                new textColumn<ExecutingQueryInfoWithCache>(grid, x => x.RunningQuery.QueryInfo, "Details", "15%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-running-queries-tooltip",
            (item: ExecutingQueryInfoWithCache, column: textColumn<ExecutingQueryInfoWithCache>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {

                let value = column.getCellValue(item);
            
                if (column.header === "Duration") {
                    value = {
                        Duration: item.RunningQuery.Duration, 
                        StartTime: item.RunningQuery.StartTime
                    }
                }

                if (!_.isUndefined(value)) {
                    const json = JSON.stringify(value, null, 4);
                    const html = highlight(json, languages.javascript, "js");
                    onValue(html, json);
                }
            });
        
        this.connectToWebSocket();
    }
    
    private killQuery(query: ExecutingQueryInfoWithCache) {
        const db = databasesManager.default.getDatabaseByName(query.DatabaseName);
        
        this.confirmationMessage("Abort the query", "Do you want to abort query for index: " + generalUtils.escapeHtml(query.IndexName) + "?", {
            html: true
        })
            .done(result => {
                if (result.can) {
                    killQueryCommand.byIndexAndQueryId(db, query.IndexName, query.RunningQuery.QueryId)
                        .execute()
                        .done(() => messagePublisher.reportSuccess("Aborting query"));
                }
            });
    }
    
    private fetchData(): JQueryPromise<pagedResult<ExecutingQueryInfoWithCache>> {
        return $.when<pagedResult<ExecutingQueryInfoWithCache>>({
            items: this.filteredData(),
            totalResultCount: this.filteredData().length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }
    
    private connectToWebSocket() {
        this.disconnectWebSocket();
        
        this.liveClient = new runningQueriesWebSocketClient(data => this.onData(data));
    }
    
    private disconnectWebSocket() {
        if (this.liveClient) {
            this.liveClient.dispose();
            this.liveClient = null;
        }
    }
    
    private filterData() {
        const dbNames = this.selectedDbNames();
        const filter = this.filter();
        
        this.filteredData(this.data().filter(d => {
            if (!_.includes(dbNames, d.DatabaseName)) {
                return false;
            }
            
            if (filter) {
                const query = (d.RunningQuery.QueryInfo as Raven.Client.Documents.Queries.IndexQuery<any>).Query.toLocaleLowerCase();
                const indexName = d.IndexName.toLocaleLowerCase();
                const filterLower = filter.toLocaleLowerCase();
                return query.includes(filterLower) || indexName.includes(filterLower);
            } else {
                return true;
            }
        }));
    }
    
    private onData(items: Array<Raven.Server.Documents.Queries.LiveRunningQueriesCollector.ExecutingQueryCollection>) {
        if (this.tailEnabled()) {
            this.data(items.flatMap(item => {
                return item.RunningQueries.map(query => {
                    return {
                        DatabaseName: item.DatabaseName,
                        IndexName: item.IndexName,
                        RunningQuery: query
                    } as ExecutingQueryInfoWithCache;
                })
            }));
            
            this.filterData();
            this.gridController().reset(false);
        }
    }
}

export = runningQueries;
