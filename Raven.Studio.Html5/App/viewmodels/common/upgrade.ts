import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import router = require("plugins/router");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class upgrade extends viewModelBase {

    inProgress = ko.observable(true);

    timeoutHandle: number ;

    attached() {
        this.poolStats();
        ko.postbox.subscribe("ActivateDatabase", (db: database) => this.dbChanged(db));
    }

    dbChanged(db:database) {
        router.navigate(appUrl.forDocuments(null, db));
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