import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");
import shell = require("viewmodels/shell");
import changeSubscription = require('models/changeSubscription');
import optional = require("common/optional");

class statistics extends viewModelBase {

    stats = ko.observable<databaseStatisticsDto>();
    indexes = ko.observableArray<KnockoutObservable<indexStatisticsDto>>();

    private refreshStatsObservable = ko.observable<number>();
    private statsSubscription: KnockoutSubscription;

    attached() {
        this.statsSubscription = this.refreshStatsObservable.throttle(3000).subscribe((e) => this.fetchStats());
        this.fetchStats();
    }

    detached() {
        super.detached();

        if (this.statsSubscription != null) {
            this.statsSubscription.dispose();
        }
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute()
                .done((result: databaseStatisticsDto) => this.processStatsResults(result));
        }

        return null;
    }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentResourceChangesApi().watchAllDocs((e) => this.refreshStatsObservable(new Date().getTime())),
            shell.currentResourceChangesApi().watchAllIndexes((e) => this.refreshStatsObservable(new Date().getTime()))
        ];
    }


    processStatsResults(results: databaseStatisticsDto) {

        // Attach some human readable dates to the indexes.
        // Attach string versions numbers with thousands separator to the indexes.
        results['CountOfDocumentsLocale'] = optional.val(results.CountOfDocuments).bind(v => v).bind(v => v.toLocaleString());
        results['CurrentNumberOfItemsToIndexInSingleBatchLocale'] = optional.val(results.CurrentNumberOfItemsToIndexInSingleBatch).bind(v => v.toLocaleString());
        results['CurrentNumberOfItemsToReduceInSingleBatchLocale'] = optional.val(results.CurrentNumberOfItemsToReduceInSingleBatch).bind(v => v.toLocaleString()); 
        results.Indexes.forEach(i=> {
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
        results.Indexes.sort((a, b) => a.PublicName < b.PublicName ? -1 : a.PublicName > b.PublicName ? 1 : 0);

        this.stats(results);

        var existingIndexes = this.indexes().map(i => i().PublicName);
        var newIndexes = results.Indexes.map(i => i.PublicName);

        var enteringIndexes = newIndexes.filter(i => !existingIndexes.contains(i));
        var exitIndexes = existingIndexes.filter(i => !newIndexes.contains(i));
        var sameIndexes = newIndexes.filter(i => existingIndexes.contains(i));

        this.indexes.pushAll(enteringIndexes.map(idx => ko.observable(results.Indexes.first(item => item.PublicName == idx))));
        this.indexes.removeAll(exitIndexes.map(idx => this.indexes().first(item => item().PublicName == idx)));

        sameIndexes.forEach(idx => {
            var newData = results.Indexes.first(item => item.PublicName == idx);
            this.indexes().first(item => item().PublicName == idx)(newData);
        });

    }
}

export = statistics;