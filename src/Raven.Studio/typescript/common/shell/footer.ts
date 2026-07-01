/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import getDatabaseFooterStatsCommand = require("commands/resources/getDatabaseFooterStatsCommand");
import changesContext = require("common/changesContext");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import license = require("models/auth/licenseModel");
import forgotTwoFactorSecretCommand = require("commands/auth/forgotTwoFactorSecretCommand");
import endpoints = require("endpoints");
import moment = require("moment");
import clientCertificateModel = require("models/auth/clientCertificateModel");
import viewHelpers = require("common/helpers/view/viewHelpers");

class footerStats {
    countOfDocuments = ko.observable<number>();
    countOfIndexes = ko.observable<number>();
    countOfStaleIndexes = ko.observable<number>();
    countOfIndexingErrors = ko.observable<number>();
    countOfEtlTasksErrors = ko.observable<number>();
    countOfAiTasksErrors = ko.observable<number>();
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
    urlForStaleIndexes = ko.pureComputed(() => appUrl.forIndexes(this.db(), null, true));
    urlForIndexingErrors = ko.pureComputed(() => appUrl.forIndexErrors(this.db()));
    urlForAbout = appUrl.forAbout();

    ssoAppUrl = ko.pureComputed<string | null>(() => {
        const cert = clientCertificateModel.certificateInfo();
        if (!cert || cert.Usage !== "SsoClient") {
            return null;
        }
        const ssoHostname = window.location.hostname.replace(/^[^.]+\./, "");
        return `${window.location.protocol}//${ssoHostname}/clusters`;
    });

    twoFactorSessionExpiration: KnockoutComputed<moment.Moment>;

    licenseClass = license.licenseCssClass;
    supportClass = license.supportCssClass;
    licenseTypeShortText = license.licenseTypeShortText;
    licenseBgColorClass = license.licenseBgColorClass;
    licenseStatusTooltip = license.licenseStatusTooltip;

    constructor() {
        this.twoFactorSessionExpiration = ko.pureComputed(() => {
            const certInfo = clientCertificateModel.certificateInfo();
            if (certInfo?.HasTwoFactor) {
                return moment.utc(certInfo.TwoFactorExpirationDate);
            } else {
                return null;
            }
        });
    }
    
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
            .then((stats) => {
                const newStats = new footerStats();
                newStats.countOfDocuments(stats.CountOfDocuments);
                newStats.countOfIndexes(stats.CountOfIndexes);
                newStats.countOfStaleIndexes(stats.CountOfStaleIndexes);
                newStats.countOfIndexingErrors(stats.CountOfIndexingErrors);
                newStats.countOfEtlTasksErrors(stats.CountOfEtlTasksErrors);
                newStats.countOfAiTasksErrors(stats.CountOfAiTasksErrors);
                this.stats(newStats);
            })
            .finally(() => this.spinners.loading(false));
    }
    
    logout() {
        viewHelpers.confirmationMessage("Log out", "Are you sure you want to log out?")
            .done(result => {
                if (result.can) {
                    new forgotTwoFactorSecretCommand()
                        .execute()
                        .done(() => {
                            window.location.href = location.origin + endpoints.global.studio._2faIndex_html;
                        });
                }
            });
    }

    refreshStats() {
        this.fetchStats()
            .then((stats) => {
                let currentStats = this.stats();
                if (!currentStats) {
                    currentStats = new footerStats();
                    this.stats(currentStats);
                    return;
                }
                currentStats.countOfDocuments(stats.CountOfDocuments);
                currentStats.countOfIndexes(stats.CountOfIndexes);
                currentStats.countOfStaleIndexes(stats.CountOfStaleIndexes);
                currentStats.countOfIndexingErrors(stats.CountOfIndexingErrors);
                currentStats.countOfEtlTasksErrors(stats.CountOfEtlTasksErrors);
                currentStats.countOfAiTasksErrors(stats.CountOfAiTasksErrors);
            });
    }

    private async fetchStats(): Promise<Raven.Server.Documents.Studio.FooterStatistics> {
        const db = this.db();

        let results: Raven.Server.Documents.Studio.FooterStatistics[];
        if (db.isSharded()) {
            results = [await new getDatabaseFooterStatsCommand(db).execute()];
        } else {
            const uniqueNodeTags = [...new Set(db.getLocations().map((location) => location.nodeTag))];
            results = await Promise.all(
                uniqueNodeTags.map((nodeTag) => new getDatabaseFooterStatsCommand(db, nodeTag).execute())
            );
        }

        const staleIndexes = [...new Set(results.flatMap((s) => s.StaleIndexes ?? []))];

        return results.reduce(
            (acc, stats) => ({
                CountOfDocuments: acc.CountOfDocuments + stats.CountOfDocuments,
                CountOfIndexes: stats.CountOfIndexes,
                CountOfIndexingErrors: acc.CountOfIndexingErrors + stats.CountOfIndexingErrors,
                CountOfEtlTasksErrors: acc.CountOfEtlTasksErrors + stats.CountOfEtlTasksErrors,
                CountOfAiTasksErrors: acc.CountOfAiTasksErrors + stats.CountOfAiTasksErrors,
                StaleIndexes: staleIndexes,
                CountOfStaleIndexes: staleIndexes.length,
            }),
            {
                CountOfDocuments: 0,
                CountOfIndexes: 0,
                CountOfIndexingErrors: 0,
                CountOfEtlTasksErrors: 0,
                CountOfAiTasksErrors: 0,
                StaleIndexes: [],
                CountOfStaleIndexes: 0,
            }
        );
    }

    private onDatabaseStats(event: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        if (!event) {
            return;
        }

        const stats = this.stats();
        stats.countOfDocuments(event.CountOfDocuments);
        stats.countOfIndexes(event.CountOfIndexes);
        stats.countOfStaleIndexes(event.CountOfStaleIndexes);
        stats.countOfIndexingErrors(event.CountOfIndexingErrors);
    }
    
}

export = footer;
