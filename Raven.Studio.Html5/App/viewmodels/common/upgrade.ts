import viewModelBase = require("viewmodels/viewModelBase");
import getReducedDatabaseStatsCommand = require("commands/resources/getReducedDatabaseStatsCommand");
import router = require("plugins/router");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class upgrade extends viewModelBase {

    inProgress = ko.observable(true);

    timeoutHandle: number ;

    attached() {
        super.attached();
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

    fetchStats(): JQueryPromise<reducedDatabaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getReducedDatabaseStatsCommand(db)
                .execute();
        }

        return null;
    }

}

export = upgrade;
