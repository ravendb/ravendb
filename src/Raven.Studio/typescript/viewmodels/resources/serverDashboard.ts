import viewModelBase = require("viewmodels/viewModelBase");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
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
                new textColumn<databaseItem>(grid, x => x.replicaFactor(), "Replica factor", "12%")
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
    totalTransferPerSecond = ko.observable<number>(0);
    
    init() {
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
                new textColumn<trafficItem>(grid, x => this.sizeFormatter(x.transferPerSecond()), "Docs data received / s", "25%")
            ];
        });
        
        this.trafficChart = new dashboardChart("#trafficChart", {
            useSeparateYScales: true,
            topPaddingProvider: key => key === "requests" ? 20 : 5
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
            key: "transfer",
            value: this.totalTransferPerSecond()
        }]);
        
        this.gridController().reset(false);
    }
    
    private updateTotals() {
        let totalRequests = 0;
        let totalTransfer = 0;

        this.table.forEach(item => {
            totalRequests += item.requestsPerSecond();
            totalTransfer += item.transferPerSecond();
        });

        this.totalRequestsPerSecond(totalRequests);
        this.totalTransferPerSecond(totalTransfer);
    }
}

class driveUsageSection {
    private table = ko.observableArray<driveUsage>();
    
    totalDocumentsSize = ko.observable<number>(0);
    
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
    }
    
    private updateTotals() {
        this.totalDocumentsSize(_.sum(this.table().map(x => x.totalDocumentsSpaceUsed())));
    }
}

class serverDashboard extends viewModelBase {

    clusterManager = clusterTopologyManager.default;
    sizeFormatter = generalUtils.formatBytesToSize;

    usingHttps = location.protocol === "https:";

    certificatesUrl = appUrl.forCertificates();
    
    trafficSection = new trafficSection();
    databasesSection = new databasesSection();
    indexingSpeedSection = new indexingSpeedSection();
    machineResourcesSection = new machineResourcesSection();
    driveUsageSection = new driveUsageSection();
    
    constructor() {
        super();
    }
    
    private createFakeDatabases(): Raven.Server.Dashboard.DatabasesInfo {
        const fakeDatabases = [] as Raven.Server.Dashboard.DatabaseInfoItem[];

        for (let i = 0; i < 25; i++) {
            const item = {
                Database: "Northwind #" + (i + 1),
                AlertsCount: 1,
                IndexesCount: _.random(1, 20),
                ErroredIndexesCount: 2,
                DocumentsCount: _.random(1000, 2000),
                ReplicaFactor: _.random(1, 3)
            } as Raven.Server.Dashboard.DatabaseInfoItem;

            fakeDatabases.push(item);
        }

        return {
            Type: "DatabasesInfo",
            Items: fakeDatabases,
            Date: moment.utc().toISOString()
        };
    }
    
    private createFakeTraffic(): Raven.Server.Dashboard.TrafficWatch {
        const fakeTraffic = [] as Raven.Server.Dashboard.TrafficWatchItem[];
        
        for (let i = 0; i < 25; i++) {
            const item = {
                Database: "Northwind #" + (i + 1),
                RequestsPerSecond: _.random(100, 1000000),
                TransferPerSecond: _.random(100, 100000)
            } as Raven.Server.Dashboard.TrafficWatchItem;
            
            fakeTraffic.push(item);
        }
        
        return {
            Type: "TrafficWatch",
            Items: fakeTraffic,
            Date: moment.utc().toISOString()
        };
    }

    private createFakeIndexingSpeed(): Raven.Server.Dashboard.IndexingSpeed {
        const fakeIndexingSpeed = [] as Raven.Server.Dashboard.IndexingSpeedItem[];

        for (let i = 0; i < 25; i++) {
            const isMap = _.random(0, 1);
            const item = {
                Database: "Northwind #" + (i + 1),
                IndexedPerSecond: isMap ? _.random(100, 200) : null,
                MappedPerSecond: isMap ? null : _.random(1000, 2000),
                ReducedPerSecond: isMap ? null : _.random(2000, 3000)
            } as Raven.Server.Dashboard.IndexingSpeedItem;

            fakeIndexingSpeed.push(item);
        }

        return {
            Type: "IndexingSpeed",
            Items: fakeIndexingSpeed,
            Date: moment.utc().toISOString()
        };
    }
    
    private createFakeMachineResources(): Raven.Server.Dashboard.MachineResources {
        return {
            TotalMemory: 64 * 1024 * 1024 * 1024,
            MemoryUsage: 32 * 1024 * 1024 * 1024,
            CpuUsage: 80,
            Type: "MachineResources",
            Date: moment.utc().toISOString()
        };
    }
    
    private createFakeDiskUsages() : Raven.Server.Dashboard.DrivesUsage {
        
        const m1 = { 
            FreeSpaceLevel: "Medium",
            FreeSpace: 10* 1024 * 1024,
            TotalCapacity: 600 * 1024 * 1024,
            MountPoint: "c:\\",
            Items: [{
                Database: "db1",
                Size: 10 * 1024 * 1024
            }, {
                Database: "db2",
                Size: 16 * 1024 * 1024
            }]
        } as Raven.Server.Dashboard.MountPointUsage;
        
        const m2 = {
            FreeSpaceLevel: "High",
            FreeSpace: 20* 1024 * 1024,
            TotalCapacity: 123 * 1024 * 1024,
            MountPoint: "d:\\",
            Items: [{
                Database: "db3",
                Size: 10 * 14 * 1024
            }, {
                Database: "db2",
                Size: 16 * 24 * 1024
            }]
        } as Raven.Server.Dashboard.MountPointUsage;
        
        return {
            Type: "DriveUsage",
            Date: moment.utc().toISOString(),
            Items: [m1, m2]
        }
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.trafficSection.init();
        this.databasesSection.init();
        this.indexingSpeedSection.init();
        
        this.machineResourcesSection.init();

        const fakeTraffic = this.createFakeTraffic();
        this.trafficSection.onData(fakeTraffic);
        
        const fakeDatabases = this.createFakeDatabases();
        this.databasesSection.onData(fakeDatabases);
        
        const fakeIndexingSpeed = this.createFakeIndexingSpeed();
        this.indexingSpeedSection.onData(fakeIndexingSpeed);
        
        const fakeMachineResources = this.createFakeMachineResources();
        this.machineResourcesSection.onData(fakeMachineResources);
        
        const fakeDriveUsage = this.createFakeDiskUsages();
        this.driveUsageSection.onData(fakeDriveUsage);

        const handler = () => {

            // traffic 
            {
                fakeTraffic.Items.forEach(x => {
                    const dx = _.random(-2000, 2000);
                    x.TransferPerSecond += dx;

                    const dy = _.random(-2000, 2000);
                    x.RequestsPerSecond += dy;
                });

                fakeTraffic.Date = moment.utc().toISOString();

                this.trafficSection.onData(fakeTraffic);

            }

            // databases
            {
                fakeDatabases.Items.forEach(x => {
                    const dx = _.random(-20, 20);
                    x.DocumentsCount += dx;
                    x.AlertsCount = _.random(0, 2);
                    x.ErroredIndexesCount = _.random(0, 2);
                    x.Online = !!_.random(0, 1);
                });

                fakeDatabases.Date = moment.utc().toISOString();

                this.databasesSection.onData(fakeDatabases);
            }

            // indexing speed
            {
                fakeIndexingSpeed.Items.forEach(x => {
                    const isMap = x.IndexedPerSecond != null;

                    const dx = _.random(-200, 1000);

                    if (isMap) {
                        x.IndexedPerSecond += dx;
                    } else {
                        x.ReducedPerSecond += dx;
                        x.MappedPerSecond -= dx;
                    }
                });

                fakeIndexingSpeed.Date = moment.utc().toISOString();

                this.indexingSpeedSection.onData(fakeIndexingSpeed);
            }

            // machine resources
            {
                fakeMachineResources.CpuUsage = _.random(0, 100);
                fakeMachineResources.MemoryUsage = _.random(10000000, 32 * 1024 * 1024 * 1024);

                fakeMachineResources.Date = moment.utc().toISOString();
                this.machineResourcesSection.onData(fakeMachineResources);
            }

            // drive usage
            {
                fakeDriveUsage.Items.forEach(item => {
                    const d1 = _.random(-10000000, 10000000);
                    item.FreeSpace += d1;

                    item.Items.forEach(x => {
                        const dx = _.random(-10000, 10000);
                        x.Size += dx;
                    });
                });

                this.driveUsageSection.onData(fakeDriveUsage);
            }

        };
        
        (this as any).handler = handler; //TODO: remove me!
        
        /*
        const interval = setInterval(handler, 1000);

        this.registerDisposable({
            dispose: () => clearInterval(interval)
        });*/
        
    }
    
}

export = serverDashboard;
