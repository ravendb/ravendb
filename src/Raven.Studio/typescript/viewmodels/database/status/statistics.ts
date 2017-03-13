import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import appUrl = require("common/appUrl");

import statsModel = require("models/database/stats/statistics");

class statistics extends viewModelBase {

    stats = ko.observable<statsModel>();
    
    private refreshStatsObservable = ko.observable<number>();
    private statsSubscription: KnockoutSubscription;

    attached() {
        super.attached();
        this.statsSubscription = this.refreshStatsObservable.throttle(3000).subscribe((e) => this.fetchStats());
        this.fetchStats();
        this.updateHelpLink('H6GYYL');
    }

    detached() {
        super.detached();

        if (this.statsSubscription != null) {
            this.statsSubscription.dispose();
        }
    }
   
    fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        var db = this.activeDatabase();

        const dbStatsTask = new getDatabaseStatsCommand(db)
            .execute();

        const indexesStatsTask = new getIndexesStatsCommand(db)
            .execute();

        return $.when<any>(dbStatsTask, indexesStatsTask)
            .done(([dbStats]: [Raven.Client.Documents.Operations.DatabaseStatistics], [indexesStats]: [Raven.Client.Documents.Indexes.IndexStats[]]) => {
                this.processStatsResults(dbStats, indexesStats);
                });
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllDocs(e => this.refreshStatsObservable(new Date().getTime())));
        this.addNotification(changesApi.watchAllIndexes((e) => this.refreshStatsObservable(new Date().getTime())))
    }

    processStatsResults(dbStats: Raven.Client.Documents.Operations.DatabaseStatistics, indexesStats: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.stats(new statsModel(dbStats, indexesStats));
    }
    
    urlForIndexPerformance(indexName: string) {
        return appUrl.forIndexPerformance(this.activeDatabase(), indexName);
    }
}

export = statistics;
