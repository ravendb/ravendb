import resource = require("models/resources/resource");
import alertArgs = require("common/alertArgs");

import abstractAction = require("common/notifications/actions/abstractAction");
import alert = require("common/notifications/actions/alert");
import operation = require("common/notifications/actions/operation");

import resourceNotificationCenterClient = require("common/resourceNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");

class notificationCenter {
    static instance = new notificationCenter();

    static readonly postponeOptions: valueAndLabelItem<number, string>[] = [
        { label: "1 hour", value: 3600 },
        { label: "6 hours", value: 6 * 3600 },
        { label: "1 day", value: 24 * 3600 },
        { label: "1 week", value: 7 * 24 * 3600 }
    ];

    showNotifications = ko.observable<boolean>(false);

    //TODO: recent errors 
    globalActions = ko.observableArray<abstractAction>();
    resourceActions = ko.observableArray<abstractAction>();

    allActions: KnockoutComputed<abstractAction[]>;

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
    }

    private initializeObservables() {
        this.allActions = ko.pureComputed(() => {
            const globalActions = this.globalActions();
            const resourceActions = this.resourceActions();

            return globalActions.concat(resourceActions);
        });

        this.totalItemsCount = ko.pureComputed(() => this.allActions().length);

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
        serverWideClient.watchAllAlerts(e => this.onAlertReceived(e, this.globalActions));
        serverWideClient.watchAllOperations(e => this.onOperationChangeReceived(e, this.globalActions));

        //TODO: append handlers for global notifications

    }

    configureForResource(client: resourceNotificationCenterClient): changeSubscription[] {
        return [
            client.watchAllAlerts(e => this.onAlertReceived(e, this.resourceActions)),
            client.watchAllOperations(e => this.onOperationChangeReceived(e, this.resourceActions))
        ];
    }

    resourceDisconnected() {
        this.resourceActions.removeAll();
    }

    private onAlertReceived(alertDto: Raven.Server.NotificationCenter.Actions.AlertRaised, alertContainer: KnockoutObservableArray<abstractAction>) {
        const existingAlert = alertContainer().find(x => x.id === alertDto.Id) as alert;
        if (existingAlert) {
            existingAlert.updateWith(alertDto);
        } else {
            const alertObject = new alert(alertDto);
            alertContainer.push(alertObject);
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Actions.OperationChanged, alertContainer: KnockoutObservableArray<abstractAction>) {
        const existingOperation = alertContainer().find(x => x.id === operationDto.Id) as operation;
        if (existingOperation) {
            existingOperation.updateWith(operationDto);
        } else {
            const operationChangedObject = new operation(operationDto);
            alertContainer.push(operationChangedObject);
        }
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress,
        TResult extends Raven.Client.Data.IOperationResult>(rs: resource,
        operationId: number,
        onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        //TODO:return this.operations.monitorOperation(rs, operationId, onProgress);
        return null; //TODO: delete me
    }

    postpone(action: abstractAction, timeInSeconds: number) {
        //TODO: send request to server 
        console.log("postpone: " + action + ", time = " + timeInSeconds);
    }

    dismiss(action: abstractAction) {
        console.log("dismiss: " + action);
        //TODO: send request to server 
    }

    /* TODO
    killOperation(operationId: number) {
       this.operations.killOperation(operationId);
    }

    dismissRecentError(alert: alertArgs) {
        this.recentErrors.dismissRecentError(alert);
    }

    showRecentErrorDialog(alert: alertArgs) {
        this.recentErrors.showRecentErrorDialog(alert);
    }*/

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).closest(".notification-center-container").length === 0
            && $(e.target).closest("#notification-toggle").length === 0;
    }
}

export = notificationCenter;