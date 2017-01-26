import resource = require("models/resources/resource");
import alertArgs = require("common/alertArgs");

import abstractAction = require("common/notifications/actions/abstractAction");
import alert = require("common/notifications/actions/alert");
import operation = require("common/notifications/actions/operation");

import resourceNotificationCenterClient = require("common/resourceNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");

import postponeActionCommand = require("commands/operations/postponeActionCommand");
import dismissActionCommand = require("commands/operations/dismissActionCommand");

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

        _.bindAll(this, "dismiss", "postpone", "killOperation", "openDetails");
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
        serverWideClient.watchAllAlerts(e => this.onAlertReceived(e, this.globalActions, null));
        serverWideClient.watchAllOperations(e => this.onOperationChangeReceived(e, this.globalActions, null));
        serverWideClient.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.globalActions, null));
    }

    configureForResource(client: resourceNotificationCenterClient): changeSubscription[] {
        return [
            client.watchAllAlerts(e => this.onAlertReceived(e, this.resourceActions, client.getResource())),
            client.watchAllOperations(e => this.onOperationChangeReceived(e, this.resourceActions, client.getResource())),
            client.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.resourceActions, client.getResource()))
        ];
    }

    resourceDisconnected() {
        this.resourceActions.removeAll();
    }

    private onAlertReceived(alertDto: Raven.Server.NotificationCenter.Actions.AlertRaised, actionsContainer: KnockoutObservableArray<abstractAction>,
        resource: resource) {
        const existingAlert = actionsContainer().find(x => x.id === alertDto.Id) as alert;
        if (existingAlert) {
            existingAlert.updateWith(alertDto);
        } else {
            const alertObject = new alert(resource, alertDto);
            actionsContainer.push(alertObject);
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Actions.OperationChanged, actionsContainer: KnockoutObservableArray<abstractAction>,
        resource: resource) {
        const existingOperation = actionsContainer().find(x => x.id === operationDto.Id) as operation;
        if (existingOperation) {
            existingOperation.updateWith(operationDto);
        } else {
            const operationChangedObject = new operation(resource, operationDto);
            actionsContainer.push(operationChangedObject);
        }
    }

    private onNotificationUpdated(notificationUpdatedDto: Raven.Server.NotificationCenter.Actions.NotificationUpdated, actionsContainer: KnockoutObservableArray<abstractAction>,
        resource: resource) {

        const existingOperation = actionsContainer().find(x => x.id === notificationUpdatedDto.ActionId) as operation;
        if (existingOperation) {
            this.removeActionFromNotificationCenter(existingOperation);
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
        const actionId = action.id;

        this.spinners.postpone.push(actionId);

        new postponeActionCommand(action.resource, actionId, timeInSeconds)
            .execute()
            .always(() => this.spinners.postpone.remove(actionId))
            .done(() => this.removeActionFromNotificationCenter(action));
    }

    dismiss(action: abstractAction) {
        const actionId = action.id;

        this.spinners.dismiss.push(actionId);

        new dismissActionCommand(action.resource, actionId)
            .execute()
            .always(() => this.spinners.dismiss.remove(actionId))
            .done(() => this.removeActionFromNotificationCenter(action));
    }

    private removeActionFromNotificationCenter(action: abstractAction) {
        this.globalActions.remove(action);
        this.resourceActions.remove(action);
    }

    killOperation(operationToKill: operation) {
        console.log("KILL: " + operation);
        //TODO: send request  + spinners
    }

    openDetails(action: abstractAction) {
        console.log("open details");
    }

    /* TODO
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