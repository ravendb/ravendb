import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");
import moment = require("moment");
import storageIoTerms = require("common/storageIoTerms");
import storageIoFormat = require("common/storageIoFormat");
import StorageCongestionDetails = Raven.Server.NotificationCenter.Notifications.Details.StorageCongestionDetails;
import SlowWriteInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowIoDetails.SlowWriteInfo;

interface summaryItem {
    name: string;
    value: string;
}

class storageCongestionDetails extends abstractPerformanceHintDetails {
    view = require("views/common/notificationCenter/detailViewer/performanceHint/storageCongestionDetails.html");

    summaryItems: summaryItem[];
    slowWritesMeasured: boolean;
    tableItems: SlowWriteInfo[];

    private gridController = ko.observable<virtualGridController<SlowWriteInfo>>();
    private columnPreview = new columnPreviewPlugin<SlowWriteInfo>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        const details = hint.details() as StorageCongestionDetails;

        this.summaryItems = storageCongestionDetails.getSummaryItems(details);
        this.slowWritesMeasured = details.SlowWritesMeasured;

        // worst first
        this.tableItems = Object.values(details.Writes ?? {}).sort((a, b) => b.DurationInSec - a.DurationInSec);
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        if (!grid) {
            return;
        }

        grid.headerVisible(true);

        grid.init(
            () => this.fetcher(),
            () => [
                new textColumn<SlowWriteInfo>(grid, (x) => x.Path, "Path", "30%", {
                    sortable: "string",
                }),
                new textColumn<SlowWriteInfo>(grid, (x) => storageIoTerms.meterTypeLabel[x.Type] ?? x.Type, "Type", "12%", {
                    sortable: "string",
                }),
                new textColumn<SlowWriteInfo>(grid, (x) => storageCongestionDetails.formatNumber(x.DataWrittenInMb, 2) + " MB", "Size", "11%", {
                    sortable: (x) => x.DataWrittenInMb,
                }),
                new textColumn<SlowWriteInfo>(grid, (x) => storageCongestionDetails.formatNumber(x.DurationInSec, 2) + " s", "Duration", "11%", {
                    sortable: (x) => x.DurationInSec,
                }),
                new textColumn<SlowWriteInfo>(grid, (x) => storageCongestionDetails.formatNumber(x.SpeedInMbPerSec, 2) + " MB/s", "Speed", "13%", {
                    sortable: (x) => x.SpeedInMbPerSec,
                }),
                new textColumn<SlowWriteInfo>(grid, (x) => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                    sortable: (x) => x.Date,
                }),
            ]
        );

        this.columnPreview.install(
            ".storageCongestionDetails",
            ".js-storage-congestion-details-tooltip",
            (details: SlowWriteInfo, column: textColumn<SlowWriteInfo>, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Date") {
                    onValue(moment.utc(details.Date), details.Date);
                } else {
                    const value = column.getCellValue(details);
                    if (value) {
                        onValue(generalUtils.escapeHtml(value), value);
                    }
                }
            }
        );
    }

    private fetcher(): JQueryPromise<pagedResult<SlowWriteInfo>> {
        return $.Deferred<pagedResult<SlowWriteInfo>>().resolve({
            items: this.tableItems,
            totalResultCount: this.tableItems.length,
        });
    }

    private static getSummaryItems(details: StorageCongestionDetails): summaryItem[] {
        const items: summaryItem[] = [];

        if (details.Cause) {
            items.push({ name: "Cause", value: details.Cause.charAt(0).toUpperCase() + details.Cause.slice(1) });
        }

        if (details.SaturatedSince) {
            const since = generalUtils.formatUtcDateAsLocal(details.SaturatedSince);
            const ago = generalUtils.formatDurationByDate(moment.utc(details.SaturatedSince), true);
            items.push({ name: "Saturated since", value: `${since} (${ago})` });
        }

        items.push({
            name: "Writes paced in the last hour",
            value: storageIoFormat.formatPercent(details.DrainShareOfLastHour),
        });

        if (details.TypicalJournalWriteLatencyInMs != null) {
            items.push({
                name: "Journal writes in the last hour",
                value: `typically ${storageIoFormat.formatMs(details.TypicalJournalWriteLatencyInMs)}, peak ${storageIoFormat.formatMs(details.PeakJournalWriteLatencyInMs)}`,
            });
        }

        items.push(
            { name: "Journal writes now", value: storageIoFormat.formatMs(details.JournalWriteLatencyInMs) },
            { name: "Data syncs", value: storageIoFormat.formatSyncCost(details.SyncCostInMs) },
            { name: "Mount point", value: details.MountPoint ?? "-" }
        );

        return items;
    }

    private static formatNumber(value: number, fractionDigits: number): string {
        return value.toLocaleString(undefined, { maximumFractionDigits: fractionDigits });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification instanceof performanceHint && notification.performanceHintReason() === "StorageCongestion";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new storageCongestionDetails(hint, center));
    }
}

export = storageCongestionDetails;
