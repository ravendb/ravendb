import getDatabaseDetailedStatsCommand = require("commands/resources/getDatabaseDetailedStatsCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import indexStalenessReasons = require("viewmodels/database/indexes/indexStalenessReasons");
import getStorageReportCommand = require("commands/database/debug/getStorageReportCommand");
import statsModel = require("models/database/stats/statistics");
import popoverUtils = require("common/popoverUtils");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database = require("models/resources/database");
import { shardingTodo } from "common/developmentHelper";

class statistics extends shardViewModelBase {

    view = require("views/database/status/statistics.html");

    stats = ko.observable<statsModel>();
    rawJsonUrl: KnockoutComputed<string>;

    private refreshStatsObservable = ko.observable<number>();
    private statsSubscription: KnockoutSubscription;

    dataLocation = ko.observable<string>();

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);
        
        this.bindToCurrentInstance("showStaleReasons");

        this.rawJsonUrl = ko.pureComputed(() => {
            const activeDatabase = this.db;
            return activeDatabase ? appUrl.forStatsRawData(activeDatabase) : null;
        });
    }

    activate() {
        return this.fetchStats();
    }
    
    attached() {
        super.attached();
        this.statsSubscription = this.refreshStatsObservable.throttle(3000).subscribe((e) => this.fetchStats());
        this.updateHelpLink('H6GYYL');
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.stats .js-size-tooltip').tooltip({
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

        popoverUtils.longWithHover($(".js-cv-tooltip"),
            {
                content: this.stats().databaseChangeVector.length === 0 ? "" : `<div>${cvTooltip}</div>`
            });
        
        popoverUtils.longWithHover($(".js-identities-header"),
            {
                content: "<div>Identities allow you to have consecutive IDs across the cluster.</div>"
            });
        
        popoverUtils.longWithHover($(".js-timeseries-segments"),
            {
                content: `<ul class="margin-top margin-right">
                              <li>
                                  <small>
                                      <strong>Time series</strong> data is stored within <strong>segments</strong>.<br>
                                      Each segment contains consecutive entries from the same time series.
                                  </small>
                              </li><br>
                              <li>
                                  <small>
                                      Segments' maximum size is 2KB.<br>
                                      Segments are added as needed when the number of entries grows,<br>
                                      or when a certain amount of time has passed since the last entry.
                                  </small>
                              </li>
                          </ul>`
            });
    }

    detached() {
        super.detached();

        if (this.statsSubscription != null) {
            this.statsSubscription.dispose();
        }
    }
   
    fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        const db = this.db;

        const dbStatsTask = new getDatabaseDetailedStatsCommand(db, this.location)
            .execute();

        const indexesStatsTask = new getIndexesStatsCommand(db, this.location)
            .execute();
 
        shardingTodo("ANY", "storage report")
        /* TODO
        const dbDataLocationTask = new getStorageReportCommand(db)
            .execute();
            
         */
        
        return $.when<any>(dbStatsTask, indexesStatsTask /* TODO, dbDataLocationTask*/)
            .done(([dbStats]: [Raven.Client.Documents.Operations.DetailedDatabaseStatistics],
                   [indexesStats]: [Raven.Client.Documents.Indexes.IndexStats[]]/* TODO,
                   [dbLocation]: [storageReportDto]*/) => {
                this.processStatsResults(dbStats, indexesStats);
                //TODO: this.dataLocation(dbLocation.BasePath);
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
        return appUrl.forIndexPerformance(this.db, indexName);
    }

    showStaleReasons(indexName: string) {
        const view = new indexStalenessReasons(this.db, indexName);
        app.showBootstrapDialog(view);
    }

    refreshStats() {
        this.fetchStats();
    }
}

export = statistics;
