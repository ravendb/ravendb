import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import shell = require("viewmodels/shell");
import changesContext = require("common/changesContext");
import changeSubscription = require('common/changeSubscription');
import optional = require("common/optional");

import statsModel = require("models/database/stats/statistics");

class statistics extends viewModelBase {

    stats = ko.observable<statsModel>();

    //TODO: noStaleIndexes = ko.computed(() => !!this.stats() && this.stats().CountOfStaleIndexesExcludingDisabledAndAbandoned == 0);

    /* TODO
    disabledIndexes = ko.computed(() => {
        if (this.stats()) {
            var stats = this.stats();
            return stats.Indexes.filter(idx => idx.Priority.indexOf("Disabled") >= 0).map(idx => idx.Name);
        }
    });*/

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


        //TODO: create subclass of databaseStatisticsDto and cast to this

        // Attach some human readable dates to the indexes.
        // Attach string versions numbers with thousands separator to the indexes.
        /* TODO:
        (<any>results)['CountOfDocumentsLocale'] = optional.val(results.CountOfDocuments).bind(v => v).bind(v => v.toLocaleString());
        (<any>results)['CurrentNumberOfItemsToIndexInSingleBatchLocale'] = optional.val(results.CurrentNumberOfItemsToIndexInSingleBatch).bind(v => v.toLocaleString());
        (<any>results)['CurrentNumberOfItemsToReduceInSingleBatchLocale'] = optional.val(results.CurrentNumberOfItemsToReduceInSingleBatch).bind(v => v.toLocaleString());
        (<any>results)['LastIndexingDateTime'] = String(optional.val(results.Indexes.map(x => x.LastIndexedTimestamp).reduce((prev, curr) => moment(prev).isAfter(moment(curr)) ? prev : curr)).bind(v => v.toHumanizedDate()));
        results.Indexes.forEach((i: any) => {
            i['CreatedTimestampText'] = optional.val(i.CreatedTimestamp).bind(v => v.toHumanizedDate());
            i['LastIndexedTimestampText'] = optional.val(i.LastIndexedTimestamp).bind(v => v.toHumanizedDate());
            i['LastQueryTimestampText'] = optional.val(i.LastQueryTimestamp).bind(v => v.toHumanizedDate());
            i['LastIndexingTimeText'] = optional.val(i.LastIndexingTime).bind(v => v.toHumanizedDate());
            i['LastReducedTimestampText'] = optional.val(i.LastReducedTimestamp).bind(v => v.toHumanizedDate());

            i['DocsCountLocale'] = optional.val(i.DocsCount).bind(v => v.toLocaleString());
            i['ReduceIndexingAttemptsLocale'] = optional.val(i.ReduceIndexingAttempts).bind(v => v.toLocaleString());
            i['ReduceIndexingErrorsLocale'] = optional.val(i.ReduceIndexingErrors).bind(v => v.toLocaleString());
            i['ReduceIndexingSuccessesLocale'] = optional.val(i.ReduceIndexingSuccesses).bind(v => v.toLocaleString());
            i['IndexingAttemptsLocale'] = optional.val(i.IndexingAttempts).bind(v => v.toLocaleString());
            i['IndexingErrorsLocale'] = optional.val(i.IndexingErrors).bind(v => v.toLocaleString());
            i['IndexingSuccessesLocale'] = optional.val(i.IndexingSuccesses).bind(v => v.toLocaleString());
        });

        results.Indexes.sort((a, b) => a.Name < b.Name ? -1 : a.Name > b.Name ? 1 : 0);

        this.stats(results);

        var existingIndexes = this.indexes().map(i => i().Name);
        var newIndexes = results.Indexes.map(i => i.Name);

        var enteringIndexes = newIndexes.filter(i => !existingIndexes.contains(i));
        var exitIndexes = existingIndexes.filter(i => !newIndexes.contains(i));
        var sameIndexes = newIndexes.filter(i => existingIndexes.contains(i));

        this.indexes.pushAll(enteringIndexes.map(idx => ko.observable(results.Indexes.first(item => item.Name == idx))));
        this.indexes.removeAll(exitIndexes.map(idx => this.indexes().first(item => item().Name == idx)));

        sameIndexes.forEach(idx => {
            var newData = results.Indexes.first(item => item.Name == idx);
            this.indexes().first(item => item().Name == idx)(newData);
        });
        */
    }
}

export = statistics;
