import app = require("durandal/app");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");

import abstractNotification = require("common/notifications/models/abstractNotification");
import alert = require("common/notifications/models/alert");
import performanceHint = require("common/notifications/models/performanceHint");
import recentError = require("common/notifications/models/recentError");
import operation = require("common/notifications/models/operation");

import databaseNotificationCenterClient = require("common/databaseNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");
import notificationCenterOperationsWatch = require("common/notifications/notificationCenterOperationsWatch");

import postponeNotificationCommand = require("commands/operations/postponeNotificationCommand");
import dismissNotificationCommand = require("commands/operations/dismissNotificationCommand");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import killOperationCommand = require("commands/operations/killOperationCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

import smugglerDatabaseDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails");
import patchDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/patchDocumentsDetails");
import deleteDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/deleteDocumentsDetails");
import indexingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/indexingDetails");
import pagingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/pagingDetails");

interface customDetailsProvider {
    supportsDetailsFor(notification: abstractNotification): boolean;
    showDetailsFor(notification: abstractNotification, notificationCenter: notificationCenter): JQueryPromise<void>;
}

interface customOperationMerger {
    merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): void;
}

class notificationCenter {
    static instance = new notificationCenter();

    static readonly postponeOptions: valueAndLabelItem<number, string>[] = [
        { label: "1 hour", value: 3600 },
        { label: "6 hours", value: 6 * 3600 },
        { label: "1 day", value: 24 * 3600 },
        { label: "1 week", value: 7 * 24 * 3600 }
    ];

    spinners = {
        dismiss: ko.observableArray<string>([]),
        postpone: ko.observableArray<string>([]),
        kill: ko.observableArray<string>([])
    }

    showNotifications = ko.observable<boolean>(false);

    globalNotifications = ko.observableArray<abstractNotification>();
    databaseNotifications = ko.observableArray<abstractNotification>();

    globalOperationsWatch = new notificationCenterOperationsWatch();
    databaseOperationsWatch = new notificationCenterOperationsWatch();

    allNotifications: KnockoutComputed<abstractNotification[]>;

    totalItemsCount: KnockoutComputed<number>;
    alertCountAnimation = ko.observable<boolean>();
    noNewNotifications: KnockoutComputed<boolean>;

    customDetailsProviders = [] as Array<customDetailsProvider>;
    customOperationMerger = [] as Array<customOperationMerger>;

    private hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.showNotifications(false);
        }
    }

    constructor() {
        this.initializeObservables();

        ko.postbox.subscribe(EVENTS.NotificationCenter.RecentError, (error: recentError) => this.onRecentError(error));

        _.bindAll(this, "dismiss", "postpone", "killOperation", "openDetails");
    }

    private initializeObservables() {

        this.customDetailsProviders.push(
            // operations:
            smugglerDatabaseDetails,
            patchDocumentsDetails,
            deleteDocumentsDetails,

            // performance hints:
            indexingDetails,
            pagingDetails
        );

        this.customOperationMerger.push(smugglerDatabaseDetails);

        this.allNotifications = ko.pureComputed(() => {
            const globalNotifications = this.globalNotifications();
            const databaseNotifications = this.databaseNotifications();

            const mergedNotifications = globalNotifications.concat(databaseNotifications);

            return _.sortBy(mergedNotifications, x => -1 * x.displayDate().unix());
        });

        this.totalItemsCount = ko.pureComputed(() => this.allNotifications().length);

        this.totalItemsCount.subscribe((count: number) => {
            if (count) {
                this.alertCountAnimation(false);
                setTimeout(() => this.alertCountAnimation(true));
            } else {
                this.alertCountAnimation(false);
            }
        });
        this.noNewNotifications = ko.pureComputed(() => {
            return this.totalItemsCount() === 0;
        });

        this.showNotifications.subscribe((show: boolean) => {
            if (show) {
                window.addEventListener("click", this.hideHandler, true);
            } else {
                window.removeEventListener("click", this.hideHandler, true);
            }
        });
    }

    setupGlobalNotifications(serverWideClient: serverNotificationCenterClient) {
        this.globalOperationsWatch.configureFor(null);
        serverWideClient.watchAllAlerts(e => this.onAlertReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllPerformanceHints(e => this.onPerformanceHintReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllOperations(e => this.onOperationChangeReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.globalNotifications, null));
    }

    configureForDatabase(client: databaseNotificationCenterClient): changeSubscription[] {
        const db = client.getDatabase();
        this.databaseOperationsWatch.configureFor(db);

        return [
            client.watchAllAlerts(e => this.onAlertReceived(e, this.databaseNotifications, db)),
            client.watchAllPerformanceHints(e => this.onPerformanceHintReceived(e, this.databaseNotifications, db)),
            client.watchAllOperations(e => this.onOperationChangeReceived(e, this.databaseNotifications, db)),
            client.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.databaseNotifications, db)),
            client.watchAllDatabaseStatsChanged(e => collectionsTracker.default.onDatabaseStatsChanged(e, db))
        ];
    }

    databaseDisconnected() {
        this.databaseNotifications.removeAll();
    }

    private onRecentError(error: recentError) {
        this.globalNotifications.push(error);
    }

    private onPerformanceHintReceived(performanceHintDto: Raven.Server.NotificationCenter.Notifications.PerformanceHint, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {
        const existingHint = notificationsContainer().find(x => x.id === performanceHintDto.Id) as performanceHint;
        if (existingHint) {
            existingHint.updateWith(performanceHintDto);
        } else {
            const hintObject = new performanceHint(database, performanceHintDto);
            notificationsContainer.push(hintObject);
        }
    }

    private onAlertReceived(alertDto: Raven.Server.NotificationCenter.Notifications.AlertRaised, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {
        const existingAlert = notificationsContainer().find(x => x.id === alertDto.Id) as alert;
        if (existingAlert) {
            existingAlert.updateWith(alertDto);
        } else {
            const alertObject = new alert(database, alertDto);
            notificationsContainer.push(alertObject);
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {
        const existingOperation = notificationsContainer().find(x => x.id === operationDto.Id) as operation;
        if (existingOperation) {
            let foundCustomMerger = false;
            for (let i = 0; i < this.customOperationMerger.length; i++) {
                const merger = this.customOperationMerger[i];
                if (merger.merge(existingOperation, operationDto)) {
                    foundCustomMerger = true;
                    break;
                }
            }

            if (!foundCustomMerger) {
                existingOperation.updateWith(operationDto);
            }
        } else {
            const operationChangedObject = new operation(database, operationDto);

            // allow custom callbacks for mergers, passing undefined to distinguish between update and create.
            this.customOperationMerger.forEach(merger => {
                merger.merge(operationChangedObject, undefined);
            });

            notificationsContainer.push(operationChangedObject);
        }

        if (operationDto.State.Status !== "InProgress") {
            // since kill request doesn't wait for actual kill, let's remove completed items
            this.spinners.kill.remove(operationDto.Id);
        }

        this.getOperationsWatch(database).onOperationChange(operationDto);
    }

    private onNotificationUpdated(notificationUpdatedDto: Raven.Server.NotificationCenter.Notifications.NotificationUpdated, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {

        const existingOperation = notificationsContainer().find(x => x.id === notificationUpdatedDto.NotificationId) as operation;
        if (existingOperation) {
            this.removeNotificationFromNotificationCenter(existingOperation);
        }
    }

    private getOperationsWatch(db: database) {
        return db ? this.databaseOperationsWatch : this.globalOperationsWatch;
    }

    monitorOperation<TProgress extends Raven.Client.Documents.Operations.IOperationProgress,
        TResult extends Raven.Client.Documents.Operations.IOperationResult>(db: database,
        operationId: number,
        onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {

        return this.getOperationsWatch(db).monitorOperation(operationId, onProgress);
    }

    postpone(notification: abstractNotification, timeInSeconds: number) {
        const notificationId = notification.id;

        this.spinners.postpone.push(notificationId);

        new postponeNotificationCommand(notification.database, notificationId, timeInSeconds)
            .execute()
            .always(() => this.spinners.postpone.remove(notificationId))
            .done(() => this.removeNotificationFromNotificationCenter(notification));
    }

    dismiss(notification: abstractNotification) {
        if (notification instanceof recentError) {
            // local dismiss
            this.globalNotifications.remove(notification);

        } else { // remote dismiss
            const notificationId = notification.id;

            const shouldDismissForever = notification instanceof performanceHint && notification.dontShowAgain();

            this.spinners.dismiss.push(notificationId);

            new dismissNotificationCommand(notification.database, notificationId, shouldDismissForever)
                .execute()
                .always(() => this.spinners.dismiss.remove(notificationId))
                .done(() => this.removeNotificationFromNotificationCenter(notification));
        }
    }

    private removeNotificationFromNotificationCenter(notification: abstractNotification) {
        this.globalNotifications.remove(notification);
        this.databaseNotifications.remove(notification);
    }

    killOperation(operationToKill: operation): void {
        const notificationId = operationToKill.id;

        this.spinners.kill.push(notificationId);

        new killOperationCommand(operationToKill.database, operationToKill.operationId())
            .execute()
            .fail(() => {
                // we don't call remove in always since killOperationCommand only delivers kill signal and doesn't wait for actual kill
                this.spinners.kill.remove(notificationId);
            });
    }

    openDetailsForOperationById(db: database, operationId: number): void {
        const existingNotification = this.getOperationById(db, operationId);
        if (existingNotification) {
            this.openDetails(existingNotification);
        } else {
            const showDialog = _.once(() => {
                // at this point operation have to exist
                this.openDetails(this.getOperationById(db, operationId));
            });

            this.monitorOperation(db, operationId, () => showDialog());
        }
    }

    private getOperationById(db: database, operationId: number) {
        const notificationsArray = db ? this.databaseNotifications() : this.globalNotifications();
        return notificationsArray.find(x => x instanceof operation && x.operationId() === operationId);
    }

    openDetails(notification: abstractNotification) {
        //TODO: it is only temporary solution to display progress/details as JSON in dialog
        const notificationCenterOpened = this.showNotifications();

        for (let i = 0; i < this.customDetailsProviders.length; i++) {
            const provider = this.customDetailsProviders[i];
            if (provider.supportsDetailsFor(notification)) {
                provider.showDetailsFor(notification, this)
                    .done(() => {
                        this.showNotifications(notificationCenterOpened);
                    });
                return;
            }
        }

        if (notification instanceof alert) {
            const currentAlert = notification as alert;
            const text = JSON.stringify(currentAlert.details(), null, 4);

            app.showBootstrapDialog(new showDataDialog("Alert", text, "javascript"))
                .done(() => {
                    this.showNotifications(notificationCenterOpened);
                });
        } else if (notification instanceof performanceHint) {
            const currentHint = notification as performanceHint;
            const text = JSON.stringify(currentHint.details(), null, 4);
            app.showBootstrapDialog(new showDataDialog("Performance Hint", text, "javascript"))
                .done(() => {
                    this.showNotifications(notificationCenterOpened);
                });
        } else if (notification instanceof operation) {
            const op = notification as operation;
            
            const dialogText = ko.pureComputed(() => {
                const completed = op.isCompleted();

                return completed ? op.result() : op.progress();
            });
            app.showBootstrapDialog(new tempStatDialog(dialogText))
                .done(() => {
                    this.showNotifications(notificationCenterOpened);
                });
        } else if (notification instanceof recentError) {
            const error = notification as recentError;
            const recentErrorDetails = {
                httpStatus: error.httpStatus(),
                details: error.details()
            };
            app.showBootstrapDialog(new tempStatDialog(recentErrorDetails))
                .done(() => {
                    this.showNotifications(notificationCenterOpened);
                });

        } else {
            throw new Error("Unable to handle details for: " + notification);
        }
    }

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).closest(".notification-center-container").length === 0
            && $(e.target).closest("#notification-toggle").length === 0;
    }
}

export = notificationCenter;