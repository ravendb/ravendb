import app = require("durandal/app");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import operation = require("common/notifications/models/operation");

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

class smugglerDatabaseDetails extends dialogViewModelBase {
    static readonly progressType = "Raven.Server.Smuggler.Documents.Data.SmugglerResult+SmugglerProgress, Raven.Server";
    static readonly resultType = "Raven.Server.Smuggler.Documents.Data.SmugglerResult, Raven.Server";

    private readonly op: operation;
    private readonly killFunction: () => void;

    detailsVisible = ko.observable<boolean>(false);

    exportItems: KnockoutComputed<Array<smugglerListItem>>;
    messages: KnockoutComputed<Array<string>>;
    killable: KnockoutComputed<boolean>;

    constructor(op: operation, killFunction: () => void) {
        super();
        this.bindToCurrentInstance("close", "toggleDetails", "killOperation");
        this.op = op;
        this.killFunction = killFunction;

        this.initObservables();
    }

    private initObservables() {
        this.killable = ko.pureComputed(() => !this.op.isCompleted());

        this.exportItems = ko.pureComputed(() => {
            const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as Raven.Server.Smuggler.Documents.Data.SmugglerProgressBase;

            if (!status) {
                return [];
            }

            const result = [] as Array<smugglerListItem>;
            result.push(this.mapToExportListItem("Documents", status.Documents));
            result.push(this.mapToExportListItem("RevisionDocuments", status.RevisionDocuments));
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

    close() {
        dialog.close(this);
    }

    killOperation() {
        //TODO: spinner
        this.killFunction();
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

    static supportsDetailsFor(op: operation) {
        if (op.status() === "InProgress") {
            return (op.progress() as any)["$type"] === smugglerDatabaseDetails.progressType;
        } else {
            return (op.result() as any)["$type"] === smugglerDatabaseDetails.resultType;
        }
    }

    static showDetailsFor(op: operation, killFunction: () => void) {
        return app.showBootstrapDialog(new smugglerDatabaseDetails(op, killFunction));
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
