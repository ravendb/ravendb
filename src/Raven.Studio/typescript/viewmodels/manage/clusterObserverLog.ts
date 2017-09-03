import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import getClusterObserverDecisionsCommand = require("commands/database/cluster/getClusterObserverDecisionsCommand");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class clusterObserverLog extends viewModelBase {

    decisions = ko.observable<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>();
    topology = clusterTopologyManager.default.topology;
    
    private gridController = ko.observable<virtualGridController<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>>();
    columnsSelector = new columnsSelector<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>();
    
    spinners = {
        refresh: ko.observable<boolean>(false)
    };
    
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
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Iteration, "Iteration", "10%"),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Database, "Database", "20%"),
                new textColumn<Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry>(grid, x => x.Message, "Message", "50%")
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
    

}

export = clusterObserverLog;
