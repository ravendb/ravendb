import resource = require("models/resources/resource");
import app = require("durandal/app");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");

import abstractNotification = require("common/notifications/models/abstractNotification");
import alert = require("common/notifications/models/alert");
import recentError = require("common/notifications/models/recentError");
import operation = require("common/notifications/models/operation");

import resourceNotificationCenterClient = require("common/resourceNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");
import notificationCenterOperationsWatch = require("common/notifications/notificationCenterOperationsWatch");

import postponeNotificationCommand = require("commands/operations/postponeNotificationCommand");
import dismissNotificationCommand = require("commands/operations/dismissNotificationCommand");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import killOperationCommand = require("commands/operations/killOperationCommand");

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
    resourceNotifications = ko.observableArray<abstractNotification>();

    globalOperationsWatch = new notificationCenterOperationsWatch();
    resourceOperationsWatch = new notificationCenterOperationsWatch();

    allNotifications: KnockoutComputed<abstractNotification[]>;

    totalItemsCount: KnockoutComputed<number>;
    alertCountAnimation = ko.observable<boolean>();
    noNewNotifications: KnockoutComputed<boolean>;

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
        this.allNotifications = ko.pureComputed(() => {
            const globalNotifications = this.globalNotifications();
            const resourceNotifications = this.resourceNotifications();

            const mergedNotifications = globalNotifications.concat(resourceNotifications);

            return _.sortBy(mergedNotifications, x => -1 * x.createdAt().unix());
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
        serverWideClient.watchAllOperations(e => this.onOperationChangeReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.globalNotifications, null));
    }

    configureForResource(client: resourceNotificationCenterClient): changeSubscription[] {
        const rs = client.getResource();
        this.resourceOperationsWatch.configureFor(rs);

        return [
            client.watchAllAlerts(e => this.onAlertReceived(e, this.resourceNotifications, rs)),
            client.watchAllOperations(e => this.onOperationChangeReceived(e, this.resourceNotifications, rs)),
            client.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.resourceNotifications, rs))
        ];
    }

    resourceDisconnected() {
        this.resourceNotifications.removeAll();
    }

    private onRecentError(error: recentError) {
        this.globalNotifications.push(error);
    }

    private onAlertReceived(alertDto: Raven.Server.NotificationCenter.Notifications.AlertRaised, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        resource: resource) {
        const existingAlert = notificationsContainer().find(x => x.id === alertDto.Id) as alert;
        if (existingAlert) {
            existingAlert.updateWith(alertDto);
        } else {
            const alertObject = new alert(resource, alertDto);
            notificationsContainer.push(alertObject);
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        resource: resource) {
        const existingOperation = notificationsContainer().find(x => x.id === operationDto.Id) as operation;
        if (existingOperation) {
            existingOperation.updateWith(operationDto);
        } else {
            const operationChangedObject = new operation(resource, operationDto);
            notificationsContainer.push(operationChangedObject);
        }

        if (operationDto.State.Status !== "InProgress") {
            // since kill request doesn't wait for actual kill, let's remove completed items
            this.spinners.kill.remove(operationDto.Id);
        }

        this.getOperationsWatch(resource).onOperationChange(operationDto);
    }

    private onNotificationUpdated(notificationUpdatedDto: Raven.Server.NotificationCenter.Notifications.NotificationUpdated, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        resource: resource) {

        const existingOperation = notificationsContainer().find(x => x.id === notificationUpdatedDto.NotificationId) as operation;
        if (existingOperation) {
            this.removeNotificationFromNotificationCenter(existingOperation);
        }
    }

    private getOperationsWatch(rs: resource) {
        return rs ? this.resourceOperationsWatch : this.globalOperationsWatch;
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress,
        TResult extends Raven.Client.Data.IOperationResult>(rs: resource,
        operationId: number,
        onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {

        return this.getOperationsWatch(rs).monitorOperation(operationId, onProgress);
    }

    postpone(notification: abstractNotification, timeInSeconds: number) {
        const notificationId = notification.id;

        this.spinners.postpone.push(notificationId);

        new postponeNotificationCommand(notification.resource, notificationId, timeInSeconds)
            .execute()
            .always(() => this.spinners.postpone.remove(notificationId))
            .done(() => this.removeNotificationFromNotificationCenter(notification));
    }

    dismiss(notification: abstractNotification) {
        if (notification instanceof recentError) {
            // local dismiss
            this.globalNotifications.remove(notification);

        } else { // remove dismiss
            const notificationId = notification.id;

            this.spinners.dismiss.push(notificationId);

            new dismissNotificationCommand(notification.resource, notificationId)
                .execute()
                .always(() => this.spinners.dismiss.remove(notificationId))
                .done(() => this.removeNotificationFromNotificationCenter(notification));
        }
    }

    private removeNotificationFromNotificationCenter(notification: abstractNotification) {
        this.globalNotifications.remove(notification);
        this.resourceNotifications.remove(notification);
    }

    killOperation(operationToKill: operation) {
        const notificationId = operationToKill.id;

        this.spinners.kill.push(notificationId);

        new killOperationCommand(operationToKill.resource as database, operationToKill.operationId())
            .execute()
            .fail(() => {
                // we don't call remove in always since killOperationCommand only delivers kill signal and doesn't wait for actual kill
                this.spinners.kill.remove(notificationId)
            });
    }

    openDetailsForOperationById(rs: resource, operationId: number): void {
        const existingNotification = this.getOperationById(rs, operationId);
        if (existingNotification) {
            this.openDetails(existingNotification);
        } else {
            const showDialog = _.once(() => {
                // at this point operation have to exist
                this.openDetails(this.getOperationById(rs, operationId));
            });

            this.monitorOperation(rs, operationId, () => showDialog());
        }
    }

    private getOperationById(rs: resource, operationId: number) {
        const notificationsArray = rs ? this.resourceNotifications() : this.globalNotifications();
        return notificationsArray.find(x => x instanceof operation && x.operationId() === operationId);
    }


    openDetails(notification: abstractNotification) {
        //TODO: it is only temporary solution to display progress/details as JSON in dialog 
        const notificationCenterOpened = this.showNotifications();


        if (notification instanceof alert) {
            const currentAlert = notification as alert;
            app.showBootstrapDialog(new tempStatDialog(currentAlert.details))
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