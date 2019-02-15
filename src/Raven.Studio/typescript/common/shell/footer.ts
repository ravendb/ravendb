/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import getDatabaseFooterStatsCommand = require("commands/resources/getDatabaseFooterStatsCommand");
import changesContext = require("common/changesContext");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import license = require("models/auth/licenseModel");

class footerStats {
    countOfDocuments = ko.observable<number>();
    countOfIndexes = ko.observable<number>();
    countOfStaleIndexes = ko.observable<number>();
    countOfIndexingErrors = ko.observable<number>();
}

class footer {
    static default = new footer();

    stats = ko.observable<footerStats>();
    private db = ko.observable<database>();
    private subscription: changeSubscription;

    spinners = {
        loading: ko.observable<boolean>(false)
    };

    urlForDocuments = ko.pureComputed(() => appUrl.forDocuments(null, this.db()));
    urlForIndexes = ko.pureComputed(() => appUrl.forIndexes(this.db()));
    urlForStaleIndexes = ko.pureComputed(() => appUrl.forIndexes(this.db(), true));
    urlForIndexingErrors = ko.pureComputed(() => appUrl.forIndexErrors(this.db()));
    urlForAbout = appUrl.forAbout();

    licenseClass = license.licenseCssClass;
    supportClass = license.supportCssClass;

    forDatabase(db: database) {
        this.db(db);
        this.stats(null);

        if (this.subscription) {
            this.subscription.off();
            this.subscription = null;
        }

        if (!db || db.disabled() || !db.relevant()) {
            return;
        }

        this.subscription = changesContext.default.databaseNotifications().watchAllDatabaseStatsChanged(e => this.onDatabaseStats(e));

        this.spinners.loading(true);

        this.fetchStats()
            .done((stats) => {
                const newStats = new footerStats();
                newStats.countOfDocuments(stats.CountOfDocuments);
                newStats.countOfIndexes(stats.CountOfIndexes);
                newStats.countOfStaleIndexes(stats.CountOfStaleIndexes);
                newStats.countOfIndexingErrors(stats.CountOfIndexingErrors);
                this.stats(newStats);
            })
            .always(() => this.spinners.loading(false));

    }

    private fetchStats(): JQueryPromise<Raven.Server.Documents.Studio.FooterStatistics> {
        return new getDatabaseFooterStatsCommand(this.db())
            .execute();
    }

    private onDatabaseStats(event: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        const stats = this.stats();
        stats.countOfDocuments(event.CountOfDocuments);
        stats.countOfIndexes(event.CountOfIndexes);
        stats.countOfStaleIndexes(event.CountOfStaleIndexes);
        stats.countOfIndexingErrors(event.CountOfIndexingErrors);
    }
    
}

export = footer;
