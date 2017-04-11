import app = require("durandal/app");
import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/abstractOperationDetails");

type smugglerListItemStatus = "processed" | "skipped" | "processing" | "pending";

type smugglerListItem = {
    name: string;
    stage: smugglerListItemStatus;
    hasReadCount: boolean;
    readCount: string;
    hasErroredCount: boolean;
    erroredCount: string;
    hasSkippedCount: boolean;
    skippedCount: string;
}

class smugglerDatabaseDetails extends abstractOperationDetails {

    detailsVisible = ko.observable<boolean>(false);

    exportItems: KnockoutComputed<Array<smugglerListItem>>;
    messages: KnockoutComputed<Array<string>>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.bindToCurrentInstance("toggleDetails");

        this.initObservables();
    }

    protected initObservables() {
        super.initObservables();

        this.exportItems = ko.pureComputed(() => {
            const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as Raven.Server.Smuggler.Documents.Data.SmugglerProgressBase;

            if (!status) {
                return [];
            }

            const result = [] as Array<smugglerListItem>;
            result.push(this.mapToExportListItem("Documents", status.Documents));
            result.push(this.mapToExportListItem("Revisions", status.RevisionDocuments));
            result.push(this.mapToExportListItem("Indexes", status.Indexes));
            result.push(this.mapToExportListItem("Transformers", status.Transformers));
            result.push(this.mapToExportListItem("Identities", status.Identities));

            let shouldUpdateToPending = false;
            result.forEach(item => {
                if (item.stage === "processing") {
                    if (shouldUpdateToPending) {
                        item.stage = "pending";
                    }

                    shouldUpdateToPending = true;
                }

                if (item.stage === "pending" || item.stage === "skipped") {
                    item.hasReadCount = false;
                    item.hasErroredCount = false;
                    item.hasSkippedCount = false;
                    item.readCount = "-";
                    item.erroredCount = "-";
                    item.skippedCount = "-";
                }
            });

            return result;
        });

        this.messages = ko.pureComputed(() => {
            if (this.op.isCompleted()) {
                const result = this.op.result() as Raven.Server.Smuggler.Documents.Data.SmugglerResult;
                return result ? result.Messages : [];
            } else {
                const progress = this.op.progress() as Raven.Server.Smuggler.Documents.Data.SmugglerResult;
                return progress ? progress.Messages : [];
            }
        });
    }

    toggleDetails() {
        this.detailsVisible(!this.detailsVisible());
    }

    private mapToExportListItem(name: string, item: Raven.Server.Smuggler.Documents.Data.SmugglerProgressBase.Counts): smugglerListItem {
        let stage: smugglerListItemStatus = "processing";
        if (item.Skipped) {
            stage = "skipped";
        } else if (item.Processed) {
            stage = "processed";
        }

        return {
            name: name,
            stage: stage,
            hasReadCount: true, // it will be reassigned in post-processing
            readCount: item.ReadCount.toLocaleString(),
            hasSkippedCount: name === "Documents",
            skippedCount: name === "Documents" ? (item as Raven.Server.Smuggler.Documents.Data.SmugglerProgressBase.CountsWithSkippedCountAndLastEtag).SkippedCount.toLocaleString() : "-",
            hasErroredCount: true, // it will be reassigned in post-processing
            erroredCount: item.ErroredCount.toLocaleString()
        } as smugglerListItem;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && (notification.taskType() === "DatabaseExport" || notification.taskType() === "DatabaseImport");
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new smugglerDatabaseDetails(op, center));
    }

    static merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean {
        if (!smugglerDatabaseDetails.supportsDetailsFor(existing)) {
            return false;
        }

        if (_.isUndefined(incoming)) {
            // object was just created  - only copy message -> message field

            if (!existing.isCompleted()) {
                const result = existing.progress() as Raven.Server.Smuggler.Documents.Data.SmugglerResult;
                result.Messages = [result.Message];
            }
            
        } else if (incoming.State.Status === "InProgress") { // if incoming operaton is in progress, then merge messages into existing item
            const incomingResult = incoming.State.Progress as Raven.Server.Smuggler.Documents.Data.SmugglerResult;
            const existingResult = existing.progress() as Raven.Server.Smuggler.Documents.Data.SmugglerResult;

            incomingResult.Messages = existingResult.Messages.concat(incomingResult.Message);
        }

        if (!_.isUndefined(incoming)) {
            existing.updateWith(incoming);
        }
    }
}

export = smugglerDatabaseDetails;
