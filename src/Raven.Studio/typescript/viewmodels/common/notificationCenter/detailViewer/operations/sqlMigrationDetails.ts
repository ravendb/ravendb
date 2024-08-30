import app = require("durandal/app");
import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

type migrationListItemStatus = "processed" | "processing" | "pending";

class migrationListItem {
    name: string;
    stage = ko.observable<migrationListItemStatus>("pending");
    hasReadCount = ko.observable<boolean>(false);
    readCount = ko.observable<string>();
    hasErroredCount = ko.observable<boolean>(false);
    erroredCount = ko.observable<string>();
    hasSkippedCount = ko.observable<boolean>(false);
    skippedCount = ko.observable<string>();
    
    constructor(name: string, item: Raven.Server.SqlMigration.Model.Counts) {
        this.name = name;
        this.updateWith(item);
    }
    
    updateWith(item: Raven.Server.SqlMigration.Model.Counts) {
        if (item.Processed) {
            this.stage("processed");
            this.hasErroredCount(true);
            this.hasSkippedCount(true);
            this.hasReadCount(true);
        }
        
        this.readCount(item.ReadCount.toLocaleString());
        this.skippedCount(item.SkippedCount.toLocaleString());
        this.erroredCount(item.ErroredCount.toLocaleString());
    }
}


class sqlMigrationDetails extends abstractOperationDetails {

    view = require("views/common/notificationCenter/detailViewer/operations/sqlMigrationDetails.html");

    detailsVisible = ko.observable<boolean>(false);
    tail = ko.observable<boolean>(true);

    items = ko.observableArray<migrationListItem>([]);
    messages: KnockoutComputed<Array<string>>;
    messagesJoined: KnockoutComputed<string>;
    previousProgressMessages: string[];
    
    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.bindToCurrentInstance("toggleDetails");

        this.initObservables();
        
        const onUpdate = () => this.updateUI();
        
        this.op.onUpdateCallbacks.push(onUpdate);
        
        this.registerDisposable({
            dispose: () => _.remove(op.onUpdateCallbacks, onUpdate)
        });
    }
    
    private updateUI() {
        if (this.op.status() === "Faulted") {
            this.items([]);
            return;
        }
        
        const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as Raven.Server.SqlMigration.Model.MigrationResult;
        if (!status) {
            this.items([]);
            return;
        }
        
        if (this.items().length) {
            Object.keys(status.PerCollectionCount).forEach(collectionName => {
                const counts = status.PerCollectionCount[collectionName];
                const existingItem = this.items().find(x => x.name === collectionName);
                existingItem.updateWith(counts);
            });
        } else {
            const items: migrationListItem[] = [];
            Object.keys(status.PerCollectionCount).forEach(collectionName => {
                const counts = status.PerCollectionCount[collectionName];
                
                items.push(new migrationListItem(collectionName, counts));
            });
            this.items(items);
        }
        
        let inProgressItemSeen = false;
        
        this.items().forEach(item => {
            if (!inProgressItemSeen && item.stage() !== "processed") {
                inProgressItemSeen = true;
                item.stage("processing");
            }
            
            if (item.stage() === "pending") {
                item.readCount("-");
                item.erroredCount("-");
                item.skippedCount(item.skippedCount() || "-");
            }
        });
        
        if (this.tail()) {
            this.syncScrolls();
        }
    }

    protected initObservables() {
        super.initObservables();

        this.messages = ko.pureComputed(() => {
            if (this.operationFailed()) {
                const errors = this.errorMessages();
                const previousMessages = this.previousProgressMessages || [];
                return previousMessages.concat(...errors);
            } else if (this.op.isCompleted()) {
                const result = this.op.result() as Raven.Server.SqlMigration.Model.MigrationResult;
                return result ? result.Messages : [];
            } else {
                const progress = this.op.progress() as Raven.Server.SqlMigration.Model.MigrationResult;
                if (progress) {
                    this.previousProgressMessages = progress.Messages;
                }
                return progress ? progress.Messages : [];
            }
        });

        this.messagesJoined = ko.pureComputed(() => this.messages() ? this.messages().join("\n") : "");
        
        this.registerDisposable(this.tail.subscribe(enabled => {
            if (enabled) {
                this.syncScrolls();
            }
        }));

        this.registerDisposable(this.operationFailed.subscribe(failed => {
            if (failed) {
                this.detailsVisible(true);
            }
        }));

        if (this.operationFailed()) {
            this.detailsVisible(true);
        }
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.updateUI();
    }

    private syncScrolls() {
        const messages = $(".migration-messages")[0];
        if (messages) {
            messages.scrollTop = messages.scrollHeight;
        }
        
        const inProgressItemIdx = this.items().findIndex(x => x.stage() === "processing");
        if (inProgressItemIdx !== -1) {
            const $itemsContainer = $(".items-container");
            const $currentItem = $(".migration-item:eq(" + inProgressItemIdx + ")", $itemsContainer);
            $itemsContainer[0].scrollTop = $currentItem[0].offsetTop - $itemsContainer.height() + 100;
        }
    }

    toggleDetails() {
        this.detailsVisible(!this.detailsVisible());
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "MigrationFromSql";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new sqlMigrationDetails(op, center));
    }

    static merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean {
        if (!sqlMigrationDetails.supportsDetailsFor(existing)) {
            return false;
        }

        const isUpdate = incoming !== undefined;

        if (!isUpdate) {
            // object was just created  - only copy message -> message field

            if (!existing.isCompleted()) {
                const result = existing.progress() as Raven.Server.SqlMigration.Model.MigrationResult;
                result.Messages = [result.Message];
            }
            
        } else if (incoming.State.Status === "InProgress") { // if incoming operaton is in progress, then merge messages into existing item
            const incomingResult = incoming.State.Progress as Raven.Server.SqlMigration.Model.MigrationResult;
            const existingResult = existing.progress() as Raven.Server.SqlMigration.Model.MigrationResult;

            incomingResult.Messages = existingResult.Messages.concat(incomingResult.Message);
        }

        if (isUpdate) {
            existing.updateWith(incoming);
        }
        
        return true;
    }
}

export = sqlMigrationDetails;
