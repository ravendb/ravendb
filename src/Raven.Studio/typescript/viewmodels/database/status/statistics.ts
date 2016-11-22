import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import shell = require("viewmodels/shell");
import changesContext = require("common/changesContext");
import changeSubscription = require('common/changeSubscription');
import optional = require("common/optional");
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
   
    fetchStats(): JQueryPromise<Raven.Client.Data.DatabaseStatistics> {
        var db = this.activeDatabase();
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((result: Raven.Client.Data.DatabaseStatistics) => this.processStatsResults(result));

    }

    createNotifications(): Array<changeSubscription> {
        //TODO: don't bother about this now
        return [
            this.changesContext.currentResourceChangesApi().watchAllDocs((e) => this.refreshStatsObservable(new Date().getTime()))
            //TODO: this.changesContext.currentResourceChangesApi().watchAllIndexes((e) => this.refreshStatsObservable(new Date().getTime()))
        ];
    }

    processStatsResults(results: Raven.Client.Data.DatabaseStatistics) {
        this.stats(new statsModel(results));
    }
    
    urlForIndexPerformance(indexName: string) {
        return appUrl.forIndexPerformance(this.activeDatabase(), indexName);
    }
}

export = statistics;
