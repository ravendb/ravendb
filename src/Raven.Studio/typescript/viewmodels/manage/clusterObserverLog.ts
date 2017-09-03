import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");

import getClusterObserverDecisionsCommand = require("commands/database/cluster/getClusterObserverDecisionsCommand");
import toggleClusterObserverCommand = require("commands/database/cluster/toggleClusterObserverCommand");

import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import ThrottleSettings = _.ThrottleSettings;

class clusterObserverLog extends viewModelBase {

    decisions = ko.observable<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>();
    topology = clusterTopologyManager.default.topology;
    observerSuspended = ko.observable<boolean>();
    
    private gridController = ko.observable<virtualGridController<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>>();
    columnsSelector = new columnsSelector<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>();
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
            
            return topologyTerm !== dataTerm;
        })
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
                totalResultCount: log.ObserverLog.length,
                items: log.ObserverLog
            } as pagedResult<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(fetcher, () =>
            [
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => clusterObserverLog.formatTimestampAsAgo(x.Date), "Date", "20%"),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Database, "Database", "20%"),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Message, "Message", "60%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".tooltip", (entry: Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry, column: textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>, e: JQueryEventObject, onValue: (context: any) => void) => {
            const value = column.getCellValue(entry);
            if (column.header === "Date") {
                // for timestamp show 'raw' date in tooltip
                onValue(entry.Date);
            } else if (!_.isUndefined(value)) {
                onValue(value);
            }
        });
    }
    
    private loadDecisions() {
        return new getClusterObserverDecisionsCommand()
            .execute()
            .done(response => {
                response.ObserverLog.reverse();
                this.decisions(response);
                this.observerSuspended(response.Suspended);
            });
    }
    
    refresh() {
        this.spinners.refresh(true);
        return this.loadDecisions()
            .done(() => this.gridController().reset(true))
            .always(() => this.spinners.refresh(false));
    }
    
    private static formatTimestampAsAgo(time: string): string {
        const dateMoment = moment.utc(time).local();
        const ago = dateMoment.diff(moment());
        return moment.duration(ago).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
    }

    suspendObserver() {
        this.confirmationMessage("Are you sure?", "Do you want to suspend cluster observer?", ["No", "Yes, suspend"])
            .done(result => {
                if (result.can) {
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
    

}

export = clusterObserverLog;
