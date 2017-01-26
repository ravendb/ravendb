import resource = require("models/resources/resource");
import alertArgs = require("common/alertArgs");

import abstractAction = require("common/notifications/actions/abstractAction");
import alertRaised = require("common/notifications/actions/alertRaised");
import operationChanged = require("common/notifications/actions/operationChanged");

import resourceNotificationCenterClient = require("common/resourceNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");

class notificationCenter {
    static instance = new notificationCenter();

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
        const existingAlert = alertContainer().find(x => x.id === alertDto.Id) as alertRaised;
        if (existingAlert) {
            existingAlert.updateWith(alertDto);
        } else {
            const alertObject = new alertRaised(alertDto);
            alertContainer.push(alertObject);
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Actions.OperationChanged, alertContainer: KnockoutObservableArray<abstractAction>) {
        const existingOperation = alertContainer().find(x => x.id === operationDto.Id) as operationChanged;
        if (existingOperation) {
            existingOperation.updateWith(operationDto);
        } else {
            const operationChangedObject = new operationChanged(operationDto);
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

    /* TODO
    killOperation(operationId: number) {
       this.operations.killOperation(operationId);
    }

    dismissOperation(operationId: number, saveOperations: boolean = true) {
        this.operations.dismissOperation(operationId, saveOperations);
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