import viewModelBase = require("viewmodels/viewModelBase");
import trafficItem = require("models/resources/serverDashboard/trafficItem");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import databaseItem = require("models/resources/serverDashboard/databaseItem");

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
                new hyperlinkColumn<databaseItem>(grid, x => x.database(), x => x.database(), "Database", "30%"), //TODO: hyperlink
                new textColumn<databaseItem>(grid, x => x.documentsCount(), "Docs #", "25%"), //TODO: format
                new textColumn<databaseItem>(grid, x => x.indexesCount(), "Index # (Error #)", "25%"), //TODO: format
                //TODO: other props
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
    private table = [] as trafficItem[];

    private gridController = ko.observable<virtualGridController<trafficItem>>();
    
    totalRequestsPerSecond = ko.observable<number>();
    totalTransferPerSecond = ko.observable<number>();
    
    init() {
        const grid = this.gridController();

        grid.headerVisible(true);
        
        grid.init((s, t) => $.when({
            totalResultCount: this.table.length,
            items: this.table
        }), () => {
            return [
                new checkedColumn(true),
                new hyperlinkColumn<trafficItem>(grid, x => x.database(), x => x.database(), "Database", "30%"), //TODO: hyperlink
                new textColumn<trafficItem>(grid, x => x.requestsPerSecond(), "Req / s", "25%"), //TODO: format
                new textColumn<trafficItem>(grid, x => x.transferPerSecond(), "MB / s", "25%"), //TODO: format
            ];
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

class serverDashboard extends viewModelBase {
    
    sizeFormatter = generalUtils.formatBytesToSize;
    
    trafficSection = new trafficSection();
    databasesSection = new databasesSection();
    
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
    
    
    compositionComplete() {
        super.compositionComplete();
        
        this.trafficSection.init();
        this.databasesSection.init();

        const fakeTraffic = this.createFakeTraffic();
        this.trafficSection.onData(fakeTraffic);
        
        const fakeDatabases = this.createFakeDatabases();
        this.databasesSection.onData(fakeDatabases);

        const interval = setInterval(() => {

            // traffic 
            {
                fakeTraffic.Items.forEach(x => {
                    const dx = _.random(-20, 20);
                    x.TransferPerSecond += dx;

                    const dy = _.random(-20, 20);
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
                });
                
                fakeDatabases.Date = moment.utc().toISOString();
                
                this.databasesSection.onData(fakeDatabases);
            }

        }, 1000);

        this.registerDisposable({
            dispose: () => clearInterval(interval)
        });
        
    }
    
}

export = serverDashboard;
