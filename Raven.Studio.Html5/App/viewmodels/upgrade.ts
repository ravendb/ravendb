import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

class upgrade extends viewModelBase {

    inProgress = ko.observable(true);

    timeoutHandle: number ;

    attached() {
        this.poolStats();
    }

    poolStats() {
        this.fetchStats()
            .done(() => {
                this.inProgress(false);
            }).fail(() => {
                this.timeoutHandle = setTimeout(this.poolStats(), 500);
            });
    }

    detached() {
        clearTimeout(this.timeoutHandle);
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute();
        }

        return null;
    }

}

export = upgrade;