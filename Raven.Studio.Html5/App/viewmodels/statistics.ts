import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");
import shell = require("viewmodels/shell");
import changeSubscription = require('models/changeSubscription');

class statistics extends viewModelBase {

    stats = ko.observable<databaseStatisticsDto>();

    private refreshStatsObservable = ko.observable<number>();
    
    private statsSubscription: KnockoutSubscription;

    attached() {
        this.statsSubscription = this.refreshStatsObservable.throttle(3000).subscribe((e) => this.fetchStats());
        this.fetchStats();
    }

    detached() {
        if (this.statsSubscription != null) {
            this.statsSubscription.dispose();
        }
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
          return new getDatabaseStatsCommand(db)
            .execute()
            .done((result: databaseStatisticsDto)=> this.processStatsResults(result));
        }

        return null;
      }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentDbChangesApi().watchAllDocs((e) => this.refreshStatsObservable(new Date().getTime())),
            shell.currentDbChangesApi().watchAllIndexes((e) => this.refreshStatsObservable(new Date().getTime()))
        ];
    }

  
  processStatsResults(results: databaseStatisticsDto) {

    // Attach some human readable dates to the indexes.
    results.Indexes.forEach(i=> {
      i['CreatedTimestampText'] = this.createHumanReadableTimeDuration(i.CreatedTimestamp);
      i['LastIndexedTimestampText'] = this.createHumanReadableTimeDuration(i.LastIndexedTimestamp);
      i['LastQueryTimestampText'] = this.createHumanReadableTimeDuration(i.LastQueryTimestamp);
      i['LastIndexingTimeText'] = this.createHumanReadableTimeDuration(i.LastIndexingTime);
      i['LastReducedTimestampText'] = this.createHumanReadableTimeDuration(i.LastReducedTimestamp);
    });

    this.stats(results);
  }

  createHumanReadableTimeDuration(aspnetJsonDate: string): string {
    if (aspnetJsonDate) {
      var dateMoment = moment(aspnetJsonDate);
      var now = moment();
      var agoInMs = dateMoment.diff(now);
      return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MMMM Do YYYY, h:mma)");
    }

    return aspnetJsonDate;
  }

}

export = statistics;