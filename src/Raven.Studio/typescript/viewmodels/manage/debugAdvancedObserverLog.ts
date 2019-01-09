import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");

import getClusterObserverDecisionsCommand = require("commands/database/cluster/getClusterObserverDecisionsCommand");
import toggleClusterObserverCommand = require("commands/database/cluster/toggleClusterObserverCommand");
import eventsCollector = require("common/eventsCollector");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import fileDownloader = require("common/fileDownloader");

class clusterObserverLog extends viewModelBase {

    filter = ko.observable<string>();
    
    decisions = ko.observable<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>();
    filteredLogs = ko.observableArray<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>([]);
    topology = clusterTopologyManager.default.topology;
    observerSuspended = ko.observable<boolean>();
    noLeader = ko.observable<boolean>(false);

    private gridController = ko.observable<virtualGridController<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>();

    termChanged: KnockoutComputed<boolean>;

    spinners = {
        refresh: ko.observable<boolean>(false),
        toggleObserver: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.termChanged = ko.pureComputed(() => {
            const topologyTerm = this.topology().currentTerm();
            const dataTerm = this.decisions().Term;
            const hasLeader = !this.noLeader();

            return hasLeader && topologyTerm !== dataTerm;
        });

        this.filter.throttle(500).subscribe(() => this.filterEntries());
    }

    activate(args: any) {
        super.activate(args);

        return this.loadDecisions();
    }

    compositionComplete(): void {
        super.compositionComplete();

        const fetcher = () => {
            const log = this.decisions();
            if (!log) {
                return $.when({
                    totalResultCount: 0,
                    items: []
                } as pagedResult<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>);
            }
            return $.when({
                totalResultCount: this.filteredLogs().length,
                items: this.filteredLogs()
            } as pagedResult<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(fetcher, () =>
            [
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                    sortable: x => x.Date
                }),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Database, "Database", "20%", {
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Message, "Message", "60%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-observer-log-tooltip", 
            (entry: Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry, 
             column: textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>, e: JQueryEventObject, 
             onValue: (context: any, valueToCopy?: string) => void) => {
            const value = column.getCellValue(entry);
            if (column.header === "Date") {
                onValue(moment.utc(entry.Date), entry.Date);
            } else if (!_.isUndefined(value)) {
                onValue(value);
            }
        });
    }
    
    private filterEntries() {
        const filter = this.filter();
        
        if (filter) {
            this.filteredLogs(this.decisions().ObserverLog.filter(x => x.Message.toLocaleLowerCase().includes(filter.toLocaleLowerCase())));
        } else {
            this.filteredLogs(this.decisions().ObserverLog);
        }
        
        if (this.gridController()) {
            this.gridController().reset(true, true);    
        }
    }

    private loadDecisions() {
        const loadTask = $.Deferred<void>();
        
        new getClusterObserverDecisionsCommand()
            .execute() 
            .done(response => {
                response.ObserverLog.reverse();
                this.decisions(response);
                this.filterEntries();
                this.observerSuspended(response.Suspended);
                this.noLeader(false);
                
                loadTask.resolve();
            })
            .fail((response: JQueryXHR) => {
                if (response && response.responseJSON ) {
                    const type = response.responseJSON['Type'];
                    if (type && type.includes("NoLeaderException")) {
                        this.noLeader(true);
                        this.filter("");
                        this.decisions({
                            Term: -1,
                            ObserverLog: [],
                            LeaderNode: null, 
                            Suspended: false,
                            Iteration: -1
                        });
                        this.filterEntries();
                        loadTask.resolve();
                        return;
                    }
                }
                
                loadTask.reject(response);
            });
        return loadTask;
    }

    refresh() {
        this.spinners.refresh(true);
        return this.loadDecisions()
            .always(() => {
                this.gridController().reset(true);
                this.spinners.refresh(false)
            });
    }

    suspendObserver() {
        this.confirmationMessage("Are you sure?", "Do you want to suspend cluster observer?", ["No", "Yes, suspend"])
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("observer-log", "suspend");
                    this.spinners.toggleObserver(true);
                    new toggleClusterObserverCommand(true)
                        .execute()
                        .always(() => {
                            this.spinners.toggleObserver(false);
                            this.refresh();
                        });
                }
            });
    }

    resumeObserver() {
        this.confirmationMessage("Are you sure?", "Do you want to resume cluster observer?", ["No", "Yes, resume"])
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("observer-log", "resume");
                    this.spinners.toggleObserver(true);
                    new toggleClusterObserverCommand(false)
                        .execute()
                        .always(() => {
                            this.spinners.toggleObserver(false);
                            this.refresh();
                        });
                }
            });
    }
    
    exportToFile() {
        const items = this.decisions().ObserverLog;
        const lines = [] as string[];
        items.forEach(v => {
            lines.push(v.Date + "," + (v.Database || "") + "," + v.Iteration + "," + v.Message);
        });

        const joinedFile = "Date,Database,Iteration,Message\r\n" + lines.join("\r\n");
        const now = moment().format("YYYY-MM-DD HH-mm");
        fileDownloader.downloadAsTxt(joinedFile, "cluster-observer-log-" + now + ".txt");
    }
}

export = clusterObserverLog;
