import viewModelBase = require("viewmodels/viewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");

import trafficItem = require("models/resources/serverDashboard/trafficItem");
import databaseItem = require("models/resources/serverDashboard/databaseItem");
import indexingSpeed = require("models/resources/serverDashboard/indexingSpeed");
import machineResources = require("models/resources/serverDashboard/machineResources");
import driveUsage = require("models/resources/serverDashboard/driveUsage");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import appUrl = require("common/appUrl");
import dashboardChart = require("models/resources/serverDashboard/dashboardChart");
import storagePieChart = require("models/resources/serverDashboard/storagePieChart");
import serverDashboardWebSocketClient = require("common/serverDashboardWebSocketClient");

class machineResourcesSection {

    cpuChart: dashboardChart;
    memoryChart: dashboardChart;
    
    totalMemory: number;
    
    resources = ko.observable<machineResources>();
    
    init() {
        this.cpuChart = new dashboardChart("#cpuChart", {
            yMaxProvider: () => 100,
            topPaddingProvider: () => 2
        });

        this.memoryChart = new dashboardChart("#memoryChart", {
            yMaxProvider: () => this.totalMemory,
            topPaddingProvider: () => 2
        });
    }
    
    onData(data: Raven.Server.Dashboard.MachineResources) {
        this.totalMemory = data.TotalMemory;
        
        this.cpuChart.onData(moment.utc(data.Date).toDate(), [{ key: "cpu", value: data.CpuUsage }]);
        this.memoryChart.onData(moment.utc(data.Date).toDate(), [{ key: "memory", value: data.MemoryUsage }]);
        
        if (this.resources()) {
            this.resources().update(data);
        } else {
            this.resources(new machineResources(data));
        }
    }
}

class indexingSpeedSection {
    indexingChart: dashboardChart;
    reduceChart: dashboardChart;
    
    private table = [] as indexingSpeed[];
    private gridController = ko.observable<virtualGridController<indexingSpeed>>();

    totalIndexedPerSecond = ko.observable<number>(0);
    totalMappedPerSecond = ko.observable<number>(0);
    totalReducedPerSecond = ko.observable<number>(0);

    init() {
        this.indexingChart = new dashboardChart("#indexingChart");
        this.reduceChart = new dashboardChart("#reduceChart");
        
        const grid = this.gridController();

        grid.headerVisible(true);

        grid.init((s, t) => $.when({
            totalResultCount: this.table.length,
            items: this.table
        }), () => {
            return [
                //TODO:  new checkedColumn(true),
                new hyperlinkColumn<indexingSpeed>(grid, x => x.database(), x => appUrl.forDocuments(null, x.database()), "Database", "30%"),
                new textColumn<indexingSpeed>(grid, x => x.indexedPerSecond() != null ? x.indexedPerSecond() : "n/a", "Indexed / sec", "15%", {
                    extraClass: item => item.indexedPerSecond() != null ? "" : "na"
                }),
                new textColumn<indexingSpeed>(grid, x => x.mappedPerSecond() != null ? x.mappedPerSecond() : "n/a", "Mapped / sec", "15%", {
                    extraClass: item => item.mappedPerSecond() != null ? "" : "na"
                }),
                new textColumn<indexingSpeed>(grid, x => x.reducedPerSecond() != null ? x.reducedPerSecond() : "n/a", "Reduced / sec", "15%", {
                    extraClass: item => item.reducedPerSecond() != null ? "" : "na"
                })
            ];
        });
    }

    onData(data: Raven.Server.Dashboard.IndexingSpeed) {
        const items = data.Items;

        const newDbs = items.map(x => x.Database);
        const oldDbs = this.table.map(x => x.database());

        const removed = _.without(oldDbs, ...newDbs);
        removed.forEach(dbName => {
            const matched = this.table.find(x => x.database() === dbName);
            _.pull(this.table, matched);
        });

        items.forEach(incomingItem => {
            const matched = this.table.find(x => x.database() === incomingItem.Database);
            if (matched) {
                matched.update(incomingItem);
            } else {
                this.table.push(new indexingSpeed(incomingItem));
            }
        });

        this.updateTotals();
        
        this.indexingChart.onData(moment.utc(data.Date).toDate(), [{
            key: "indexing", value: this.totalIndexedPerSecond() 
        }]);
        
        this.reduceChart.onData(moment.utc(data.Date).toDate(), [
            { key: "map", value: this.totalMappedPerSecond() },
            { key: "reduce", value: this.totalReducedPerSecond() }
        ]);

        this.gridController().reset(false);
    }

    private updateTotals() {
        let totalIndexed = 0;
        let totalMapped = 0;
        let totalReduced = 0;

        this.table.forEach(item => {
            totalIndexed += item.indexedPerSecond() || 0;
            totalMapped += item.mappedPerSecond() || 0;
            totalReduced += item.reducedPerSecond() || 0;
        });

        this.totalIndexedPerSecond(totalIndexed);
        this.totalMappedPerSecond(totalMapped);
        this.totalReducedPerSecond(totalReduced);
    }
}

class databasesSection {
    private table = [] as databaseItem[];
    private gridController = ko.observable<virtualGridController<databaseItem>>();
    
    totalOfflineDatabases = ko.observable<number>(0);
    totalOnlineDatabases = ko.observable<number>(0);
    totalDatabases: KnockoutComputed<number>;
    
    constructor() {
        this.totalDatabases = ko.pureComputed(() => this.totalOnlineDatabases() + this.totalOfflineDatabases());
    }
    
    init() {
        const grid = this.gridController();

        grid.headerVisible(true);

        grid.init((s, t) => $.when({
            totalResultCount: this.table.length,
            items: this.table
        }), () => {
            return [
                new hyperlinkColumn<databaseItem>(grid, x => x.database(), x => appUrl.forDocuments(null, x.database()), "Database", "30%"), 
                new textColumn<databaseItem>(grid, x => x.documentsCount(), "Docs #", "25%"),
                new textColumn<databaseItem>(grid, 
                        x => x.indexesCount() + ( x.erroredIndexesCount() ? ' (<span class=\'text-danger\'>' + x.erroredIndexesCount() + '</span>)' : '' ), 
                        "Index # (Error #)", 
                        "20%",
                        {
                            useRawValue: () => true
                        }),
                new textColumn<databaseItem>(grid, x => x.alertsCount(), "Alerts #", "12%", {
                    extraClass: item => item.alertsCount() ? 'has-alerts' : ''
                }), 
                new textColumn<databaseItem>(grid, x => x.replicationFactor(), "Replica factor", "12%")
            ];
        });
    }
    
    onData(data: Raven.Server.Dashboard.DatabasesInfo) {
        const items = data.Items;

        const newDbs = items.map(x => x.Database);
        const oldDbs = this.table.map(x => x.database());

        const removed = _.without(oldDbs, ...newDbs);
        removed.forEach(dbName => {
            const matched = this.table.find(x => x.database() === dbName);
            _.pull(this.table, matched);
        });

        items.forEach(incomingItem => {
            const matched = this.table.find(x => x.database() === incomingItem.Database);
            if (matched) {
                matched.update(incomingItem);
            } else {
                this.table.push(new databaseItem(incomingItem));
            }
        });
        
        this.updateTotals();
        
        this.gridController().reset(false);
    }
    
    private updateTotals() {
        let totalOnline = 0;
        let totalOffline = 0;
        
        this.table.forEach(item => {
            if (item.online()) {
                totalOnline++;
            } else {
                totalOffline++;
            }
        });
        
        this.totalOnlineDatabases(totalOnline);
        this.totalOfflineDatabases(totalOffline);
    }
}

class trafficSection {
    private sizeFormatter = generalUtils.formatBytesToSize;
    
    private table = [] as trafficItem[];
    private trafficChart: dashboardChart;

    private gridController = ko.observable<virtualGridController<trafficItem>>();
    
    totalRequestsPerSecond = ko.observable<number>(0);
    totalWritesPerSecond = ko.observable<number>(0);
    totalDataWritesPerSecond = ko.observable<number>(0);
    
    init()  {
        const grid = this.gridController();

        grid.headerVisible(true);
        
        grid.init((s, t) => $.when({
            totalResultCount: this.table.length,
            items: this.table
        }), () => {
            return [
                //TODO: new checkedColumn(true),
                new hyperlinkColumn<trafficItem>(grid, x => x.database(), x => appUrl.forDocuments(null, x.database()), "Database", "30%"),
                new textColumn<trafficItem>(grid, x => x.requestsPerSecond(), "Requests / s", "25%"),
                new textColumn<trafficItem>(grid, x => x.writesPerSecond(), "Writes / s", "25%")
            ];
        });
        
        this.trafficChart = new dashboardChart("#trafficChart", {
            useSeparateYScales: true,
            topPaddingProvider: key => (key === "writes") ? 20 : 5
        });
    }
    
    onData(data: Raven.Server.Dashboard.TrafficWatch) {
        const items = data.Items;
        const newDbs = items.map(x => x.Database);
        const oldDbs = this.table.map(x => x.database());
        
        const removed = _.without(oldDbs, ...newDbs);
        removed.forEach(dbName => {
           const matched = this.table.find(x => x.database() === dbName);
           _.pull(this.table, matched);
        });
        
        items.forEach(incomingItem => {
            const matched = this.table.find(x => x.database() === incomingItem.Database);
            if (matched) {
                matched.update(incomingItem);
            } else {
                this.table.push(new trafficItem(incomingItem));
            }
        });
        
        this.updateTotals();
        
        this.trafficChart.onData(moment.utc(data.Date).toDate(), [{
            key: "requests",
            value: this.totalRequestsPerSecond()
        }, {
            key: "writes",
            value: this.totalWritesPerSecond()
        }]);
        
        this.gridController().reset(false);
    }
    
    private updateTotals() {
        let totalRequests = 0;
        let writesPerSecond = 0;
        let dataWritesPerSecond = 0;

        this.table.forEach(item => {
            totalRequests += item.requestsPerSecond();
            writesPerSecond += item.writesPerSecond();
            dataWritesPerSecond += item.dataWritesPerSecond();
        });

        this.totalRequestsPerSecond(totalRequests);
        this.totalWritesPerSecond(writesPerSecond);
        this.totalDataWritesPerSecond(dataWritesPerSecond);
    }
}

class driveUsageSection {
    private table = ko.observableArray<driveUsage>();
    private storageChart: storagePieChart;
    
    totalDocumentsSize = ko.observable<number>(0);
    
    init() {
        this.storageChart = new storagePieChart("#storageChart");
    }
    
    onData(data: Raven.Server.Dashboard.DrivesUsage) {
        const items = data.Items;
        const newMountPoints = items.map(x => x.MountPoint);
        const oldMountPoints = this.table().map(x => x.mountPoint());

        const removed = _.without(oldMountPoints, ...newMountPoints);
        removed.forEach(name => {
            const matched = this.table().find(x => x.mountPoint() === name);
            this.table.remove(matched);
        });

        items.forEach(incomingItem => {
            const matched = this.table().find(x => x.mountPoint() === incomingItem.MountPoint);
            if (matched) {
                matched.update(incomingItem);
            } else {
                const usage = new driveUsage(incomingItem);
                this.table.push(usage);
            }
        });

        this.updateTotals();
        this.updateChart(data);
    }
    
    private updateChart(data: Raven.Server.Dashboard.DrivesUsage) {
        const cache = new Map<string, number>();
        
        // group by database size
        data.Items.forEach(mountPointUsage => {
            mountPointUsage.Items.forEach(item => {
                if (cache.has(item.Database)) {
                    cache.set(item.Database, item.Size + cache.get(item.Database));
                } else {
                    cache.set(item.Database, item.Size);
                }
            });
        });
        
        const result = [] as Raven.Server.Dashboard.DatabaseDiskUsage[];
        
        cache.forEach((value, key) => {
            result.push({
                Database: key,
                Size: value
            });
        });
        
        this.storageChart.onData(result);
    }
    
    private updateTotals() {
        this.totalDocumentsSize(_.sum(this.table().map(x => x.totalDocumentsSpaceUsed())));
    }
}

class serverDashboard extends viewModelBase {

    liveClient = ko.observable<serverDashboardWebSocketClient>();
    
    clusterManager = clusterTopologyManager.default;
    sizeFormatter = generalUtils.formatBytesToSize;

    usingHttps = location.protocol === "https:";

    certificatesUrl = appUrl.forCertificates();
    
    trafficSection = new trafficSection();
    databasesSection = new databasesSection();
    indexingSpeedSection = new indexingSpeedSection();
    machineResourcesSection = new machineResourcesSection();
    driveUsageSection = new driveUsageSection();
    
    compositionComplete() {
        super.compositionComplete();
        
        this.initSections();
        
        this.enableLiveView();
    }
    
    private initSections() {
        this.trafficSection.init();
        this.databasesSection.init();
        this.indexingSpeedSection.init();
        this.machineResourcesSection.init();
        this.driveUsageSection.init();
    }
    
    private enableLiveView() {
        this.liveClient(new serverDashboardWebSocketClient(d => this.onData(d)));
    }
    
    private onData(data: Raven.Server.Dashboard.AbstractDashboardNotification) {
        switch (data.Type) {
            case "DriveUsage":
                this.driveUsageSection.onData(data as Raven.Server.Dashboard.DrivesUsage);
                break;
            case "MachineResources":
                this.machineResourcesSection.onData(data as Raven.Server.Dashboard.MachineResources);
                break;
            case "TrafficWatch":
                this.trafficSection.onData(data as Raven.Server.Dashboard.TrafficWatch);
                break;
            case "DatabasesInfo":
                this.databasesSection.onData(data as Raven.Server.Dashboard.DatabasesInfo);
                break;
            case "IndexingSpeed":
                this.indexingSpeedSection.onData(data as Raven.Server.Dashboard.IndexingSpeed);
                break;
            default:
                throw new Error("Unhandled notification type: " + data.Type);
        }
    }
}

export = serverDashboard;
