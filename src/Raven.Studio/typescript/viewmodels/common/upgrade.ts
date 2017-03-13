import EVENTS = require("common/constants/events");
import viewModelBase = require("viewmodels/viewModelBase");
import databaseActivatedEventArgs = require("viewmodels/resources/databaseActivatedEventArgs");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import router = require("plugins/router");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class upgrade extends viewModelBase {

    inProgress = ko.observable(true);

    timeoutHandle: number ;

    attached() {
        super.attached();
        this.poolStats();

        ko.postbox.subscribe(EVENTS.Database.Activate, (e: databaseActivatedEventArgs) => {
            this.dbChanged(e.database); 
        });
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

    fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute();
        }

        return null;
    }

}

export = upgrade;
