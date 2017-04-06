/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import changesContext = require("common/changesContext");
import changeSubscription = require("common/changeSubscription");

class footerStats {
    countOfDocuments = ko.observable<number>();
    countOfIndexes = ko.observable<number>();
    countOfStaleIndexes = ko.observable<number>();
}

class footer {
    static default = new footer();

    stats = ko.observable<footerStats>();
    private db: database;
    private subscription: changeSubscription;

    forDatabase(db: database) {
        this.db = db;
        this.stats(null);

        if (this.subscription) {
            this.subscription.off();
            this.subscription = null;
        }

        if (!db) {
            return;
        }

        this.subscription = changesContext.default.databaseNotifications().watchAllDatabaseStatsChanged(e => this.onDatabaseStats(e));

        this.fetchStats()
            .done((stats) => {
                const newStats = new footerStats();
                newStats.countOfDocuments(stats.CountOfDocuments);
                newStats.countOfIndexes(stats.Indexes.length);
                newStats.countOfStaleIndexes(stats.Indexes.filter(x => x.IsStale).length);
                this.stats(newStats);
            });

    }

    private fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        return new getDatabaseStatsCommand(this.db)
            .execute();
    }

    private onDatabaseStats(event: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        const stats = this.stats();
        stats.countOfDocuments(event.CountOfDocuments);
        stats.countOfIndexes(event.CountOfIndexes);
        stats.countOfStaleIndexes(event.CountOfStaleIndexes);
    }
    
}

export = footer;