import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseDetailedStatsCommand = require("commands/resources/getDatabaseDetailedStatsCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import indexStalenessReasons = require("viewmodels/database/indexes/indexStalenessReasons");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");
import statsModel = require("models/database/stats/statistics");
import popoverUtils = require("common/popoverUtils");
import getIdentitiesCommand = require("commands/database/debug/getIdentitiesCommand");

type identityItem = { Prefix: string, Value: number };

class statistics extends viewModelBase {

    stats = ko.observable<statsModel>();
    identities = ko.observableArray<identityItem>([]);
    rawJsonUrl: KnockoutComputed<string>;

    private refreshStatsObservable = ko.observable<number>();
    private statsSubscription: KnockoutSubscription;

    dataLocation = ko.observable<string>();

    constructor() {
        super();
        
        this.bindToCurrentInstance("showStaleReasons");
    }
    
    attached() {
        super.attached();
        this.statsSubscription = this.refreshStatsObservable.throttle(3000).subscribe((e) => this.fetchStats());
        this.fetchStats();
        this.updateHelpLink('H6GYYL');

        this.rawJsonUrl = ko.pureComputed(() => {
            return appUrl.forStatsRawData(this.activeDatabase());
        });
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.stats .size-tooltip').tooltip({
            container: "body",
            html: true,
            placement: "right",
            title: () => {
                return `Data: <strong>${this.stats().dataSizeOnDisk}</strong><br />
                Temp: <strong>${this.stats().tempBuffersSizeOnDisk}</strong><br />
                Total: <strong>${this.stats().totalSizeOnDisk}</strong>`
            }
        });

        const cvTooltip = this.stats().databaseChangeVector.map(cv => `<small>${cv.fullFormat}</small>`)
            .join("<br>");

        popoverUtils.longWithHover($(".cv-tooltip"),
            {
                content: `<div>${cvTooltip}</div>`
            });
        
        popoverUtils.longWithHover($(".identities-header"),
            {
                content: "<div>Identities allow you to have consecutive IDs across the cluster.</div>"
            });
    }

    detached() {
        super.detached();

        if (this.statsSubscription != null) {
            this.statsSubscription.dispose();
        }
    }
   
    fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        const db = this.activeDatabase();

        const dbStatsTask = new getDatabaseDetailedStatsCommand(db)
            .execute();

        const indexesStatsTask = new getIndexesStatsCommand(db)
            .execute();
 
        const dbDataLocationTask = new getStorageReportCommand(db)
            .execute();
        
        const identitiesTask = new getIdentitiesCommand(db)
            .execute();
        
        return $.when<any>(dbStatsTask, indexesStatsTask, dbDataLocationTask, identitiesTask)
            .done(([dbStats]: [Raven.Client.Documents.Operations.DetailedDatabaseStatistics],
                   [indexesStats]: [Raven.Client.Documents.Indexes.IndexStats[]],
                   [dbLocation]: [storageReportDto],
                   [identities]: [dictionary<number>]) => {
                this.processStatsResults(dbStats, indexesStats);
                this.dataLocation(dbLocation.BasePath);
                
                const mappedIdentities = _.map(identities, (value, key) => {
                    return {
                        Prefix: key,
                        Value: value
                    } as identityItem;
                });
                
                this.identities(_.sortBy(mappedIdentities, x => x.Prefix.toLocaleLowerCase()));
            });
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllDocs(e => this.refreshStatsObservable(new Date().getTime())));
        this.addNotification(changesApi.watchAllIndexes((e) => this.refreshStatsObservable(new Date().getTime())))
    }

    processStatsResults(dbStats: Raven.Client.Documents.Operations.DetailedDatabaseStatistics, indexesStats: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.stats(new statsModel(dbStats, indexesStats));
    }
    
    urlForIndexPerformance(indexName: string) {
        return appUrl.forIndexPerformance(this.activeDatabase(), indexName);
    }

    showStaleReasons(indexName: string) {
        const view = new indexStalenessReasons(this.activeDatabase(), indexName);
        app.showBootstrapDialog(view);
    }
}

export = statistics;
