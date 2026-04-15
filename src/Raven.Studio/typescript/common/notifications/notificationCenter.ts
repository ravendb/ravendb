import EVENTS = require("common/constants/events");
import database = require("models/resources/database");

import abstractNotification = require("common/notifications/models/abstractNotification");
import viewHelpers = require("common/helpers/view/viewHelpers");
import alert = require("common/notifications/models/alert");
import performanceHint = require("common/notifications/models/performanceHint");
import recentError = require("common/notifications/models/recentError");
import attachmentUpload = require("common/notifications/models/attachmentUpload");
import recentLicenseLimitError = require("common/notifications/models/recentLicenseLimitError");
import operation = require("common/notifications/models/operation");

import databaseNotificationCenterClient = require("common/databaseNotificationCenterClient");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import changeSubscription = require("common/changeSubscription");
import notificationCenterOperationsWatch = require("common/notifications/notificationCenterOperationsWatch");

import postponeNotificationCommand = require("commands/operations/postponeNotificationCommand");
import dismissNotificationCommand = require("commands/operations/dismissNotificationCommand");
import killOperationCommand = require("commands/operations/killOperationCommand");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

import smugglerDatabaseDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails");
import sqlMigrationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/sqlMigrationDetails");
import patchDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/patchDocumentsDetails");
import reshardingDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/reshardingDetails");
import virtualBulkInsertDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualBulkInsertDetails");
import virtualBulkInsertFailuresDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualBulkInsertFailuresDetails");
import virtualUpdateByQueryDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualUpdateByQueryDetails");
import virtualUpdateByQueryFailuresDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualUpdateByQueryFailuresDetails");
import virtualDeleteByQueryDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualDeleteByQueryDetails");
import virtualDeleteByQueryFailuresDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualDeleteByQueryFailuresDetails");
import bulkInsertDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/bulkInsertDetails");
import revertRevisionsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/revertRevisionsDetails");
import enforceRevisionsConfigurationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/enforceRevisionsConfigurationDetails");
import adoptOrphanedRevisionsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/adoptOrphanedRevisionsDetails");
import replayTransactionCommandsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/replayTransactionCommandsDetails");
import deleteDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/deleteDocumentsDetails");
import generateClientCertificateDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/generateClientCertificateDetails");
import compactDatabaseDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/compactDatabaseDetails");
import indexingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/indexingDetails");
import slowSqlDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/slowSqlDetails");
import indexingReferencesDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/indexingReferencesDetails");
import slowIoDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/slowIoDetails");
import pagingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/pagingDetails");
import hugeDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/hugeDocumentsDetails");
import newVersionAvailableDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/newVersionAvailableDetails");
import etlTransformOrLoadErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/etlTransformOrLoadErrorDetails");
import remoteAttachmentErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/remoteAttachmentErrorDetails");
import genericAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/genericAlertDetails");
import recentErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/recentErrorDetails");
import notificationCenterSettings = require("common/notifications/notificationCenterSettings");
import licenseLimitDetails = require("viewmodels/common/notificationCenter/detailViewer/licenseLimitDetails");
import requestLatencyDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/requestLatencyDetails");
import transactionCommandsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/transactionCommandsDetails");
import dumpRawIndexDataDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/dumpRawIndexDataDetails");
import validateSchemaDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/validateSchemaDetails");

import studioSettings = require("common/settings/studioSettings");
import optimizeIndexDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/optimizeIndexDetails");
import mismatchedReferenceLoadDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/mismatchedReferenceLoadDetails");
import blockingTombstonesDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/blockingTombstonesDetails");
import serverLimitsDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/serverLimitsDetails");
import queueSinkErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/queueSinkErrorDetails");
import conflictExceededDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/conflictExceededDetails");
import complexFieldsAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/complexFieldsAlertDetails");
import cpuCreditsBalanceDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/cpuCreditsBalanceDetails");
import groupedVirtualNotification = require("common/notifications/models/groupedVirtualNotification");
import typeUtils = require("common/typeUtils");
import aiAgentExceededTokenThreshold = require("viewmodels/common/notificationCenter/detailViewer/alerts/aiAgentExceededTokenThreshold");

interface detailsProvider {
    supportsDetailsFor(notification: abstractNotification): boolean;
    showDetailsFor(notification: abstractNotification, notificationCenter: notificationCenter): JQueryPromise<void> | void;
}

interface customOperationMerger {
    merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean;
}

interface customOperationHandler {
    tryHandle(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
              database: database, callbacks: { spinnersCleanup: () => void, onChange: () => void }): boolean;
}

class notificationCenter {
    static instance = new notificationCenter();

    static readonly postponeOptions = notificationCenterSettings.postponeOptions;
    private static readonly maxRecentOperations = 50;

    spinners = {
        dismiss: ko.observableArray<string>([]),
        postpone: ko.observableArray<string>([]),
        kill: ko.observableArray<string>([])
    };

    showNotifications = ko.observable<boolean>(false);
    pinNotifications = ko.observable<boolean>(false);
    
    isShowingAllNotifications = ko.observable<boolean>(false);
    static readonly numberOfNotificationsToShow = 300;
    
    includeInDom = ko.observable<boolean>(false); // to avoid RavenDB-10660

    globalNotifications = ko.observableArray<abstractNotification>();
    databaseNotifications = ko.observableArray<abstractNotification>();

    globalOperationsWatch = new notificationCenterOperationsWatch();
    databaseOperationsWatch = new notificationCenterOperationsWatch();

    allNotifications: KnockoutComputed<abstractNotification[]>;
    notificationsToShow: KnockoutComputed<abstractNotification[]>;
    
    visibleNotifications: KnockoutComputed<abstractNotification[]>;
    visibleNotificationsTrimmed: KnockoutComputed<abstractNotification[]>;

    totalItemsCount: KnockoutComputed<number>;
    successItemsCount: KnockoutComputed<number>;
    infoItemsCount: KnockoutComputed<number>;
    warningItemsCount: KnockoutComputed<number>;
    errorItemsCount: KnockoutComputed<number>;

    alertCountAnimation = ko.observable<boolean>();
    noNewNotifications: KnockoutComputed<boolean>;

    severityFilter = ko.observable<Raven.Server.NotificationCenter.Notifications.NotificationSeverity>();

    detailsProviders: detailsProvider[] = [];
    customOperationMerger: customOperationMerger[] = [];
    customOperationHandler: customOperationHandler[] = [];
    private pendingOperationDetails = new Set<string>();
    private recentOperations = new Map<string, Raven.Server.NotificationCenter.Notifications.OperationChanged>();
    private currentDatabaseName: string;

    private hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.showNotifications(false);
        }
    };

    constructor() {
        this.initializeObservables();

        ko.postbox.subscribe(EVENTS.NotificationCenter.RecentError, (error: recentError) => this.onRecentError(error));
        ko.postbox.subscribe(EVENTS.NotificationCenter.OpenNotification, (error: recentError) => this.openDetails(error));

        _.bindAll(this, "dismiss", "postpone", "killOperation", "openDetails", "killAttachmentUpload");
    }

    private initializeObservables() {

        this.detailsProviders.push(
            licenseLimitDetails,
            // recent errors: 
            recentErrorDetails,

            // operations:
            smugglerDatabaseDetails,
            sqlMigrationDetails,
            patchDocumentsDetails,
            reshardingDetails,
            generateClientCertificateDetails,
            deleteDocumentsDetails,
            bulkInsertDetails,
            revertRevisionsDetails,
            enforceRevisionsConfigurationDetails,
            adoptOrphanedRevisionsDetails,
            compactDatabaseDetails,
            replayTransactionCommandsDetails,
            transactionCommandsDetails,
            dumpRawIndexDataDetails,
            optimizeIndexDetails,
            validateSchemaDetails,
            
            // virtual operations:
            virtualBulkInsertDetails,
            virtualBulkInsertFailuresDetails,
            virtualUpdateByQueryDetails,
            virtualUpdateByQueryFailuresDetails,
            virtualDeleteByQueryDetails,
            virtualDeleteByQueryFailuresDetails,

            // performance hints:
            indexingDetails,
            slowSqlDetails,
            indexingReferencesDetails,
            slowIoDetails,
            pagingDetails,
            requestLatencyDetails,
            hugeDocumentsDetails,
            
            // alerts:
            newVersionAvailableDetails,
            etlTransformOrLoadErrorDetails,
            mismatchedReferenceLoadDetails,
            blockingTombstonesDetails,
            queueSinkErrorDetails,
            cpuCreditsBalanceDetails,
            serverLimitsDetails,
            conflictExceededDetails,
            complexFieldsAlertDetails,
            aiAgentExceededTokenThreshold,
            remoteAttachmentErrorDetails,
            genericAlertDetails  // leave it as last item on this list - this is fallback handler for all alert types
        );

        this.customOperationMerger.push(smugglerDatabaseDetails);
        this.customOperationMerger.push(sqlMigrationDetails);
        this.customOperationMerger.push(compactDatabaseDetails);
        
        this.customOperationHandler.push(bulkInsertDetails);
        this.customOperationHandler.push(patchDocumentsDetails);
        this.customOperationHandler.push(deleteDocumentsDetails);
        this.customOperationHandler.push(transactionCommandsDetails);

        this.allNotifications = ko.pureComputed(() => {
            const globalNotifications = this.globalNotifications();
            const databaseNotifications = this.databaseNotifications();

            const mergedNotifications = globalNotifications.concat(databaseNotifications);

            return typeUtils.sortBy(mergedNotifications, x => -1 * x.displayDate().unix());
        });

        this.visibleNotifications = ko.pureComputed(() => {
            const severity = this.severityFilter();
            const allNotifications = this.allNotifications();
            if (!severity) {
                return allNotifications;
            }

            return allNotifications.filter(x => x.severity() === severity);
        });

        this.visibleNotificationsTrimmed = ko.pureComputed(() =>
            this.visibleNotifications().slice(0, notificationCenter.numberOfNotificationsToShow));

        this.notificationsToShow = ko.pureComputed(() =>
            this.isShowingAllNotifications() ? this.visibleNotifications() : this.visibleNotificationsTrimmed());

        this.totalItemsCount = ko.pureComputed(() => this.allNotifications().length);

        const bySeverityCounter = (severity: Raven.Server.NotificationCenter.Notifications.NotificationSeverity) => {
            return ko.pureComputed(() => this.allNotifications().filter(x => x.severity() === severity).length);
        };

        this.successItemsCount = bySeverityCounter("Success");
        this.warningItemsCount = bySeverityCounter("Warning");
        this.infoItemsCount = bySeverityCounter("Info");
        this.errorItemsCount = bySeverityCounter("Error");

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
    }

    initialize() {
        $("#notification-center").on('transitionend', () => {
            if (!this.showNotifications()) {
                this.includeInDom(false);
            }
        });
        
        this.showNotifications.subscribe((show: boolean) => {
            if (show) {
                this.includeInDom(true);
                this.isShowingAllNotifications(false);
                window.addEventListener("click", this.hideHandler, true);
            } else {
                window.removeEventListener("click", this.hideHandler, true);
            }
        });

        this.pinNotifications.subscribe((pinned: boolean) => {
            studioSettings.default.globalSettings()
                .done(settings => {
                    settings.pinnedNotifications.setValue(pinned);
                });
        });

        studioSettings.default.globalSettings()
            .done(settings => {
                const pinnedNotificationsFromSettings = settings.pinnedNotifications.getValue();
                this.pinNotifications(pinnedNotificationsFromSettings);
            });
    }
    
    setupGlobalNotifications(serverWideClient: serverNotificationCenterClient) {
        this.globalOperationsWatch.configureFor(null);
        serverWideClient.watchAllAlerts(e => this.onAlertReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllPerformanceHints(e => this.onPerformanceHintReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllOperations(e => this.onOperationChangeReceived(e, this.globalNotifications, null));
        serverWideClient.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.globalNotifications));
    }

    configureForDatabase(client: databaseNotificationCenterClient): changeSubscription[] {
        const db = client.getDatabase();
        this.currentDatabaseName = db.name;
        this.databaseOperationsWatch.configureFor(db);

        return [
            client.watchAllAlerts(e => this.onAlertReceived(e, this.databaseNotifications, db)),
            client.watchAllPerformanceHints(e => this.onPerformanceHintReceived(e, this.databaseNotifications, db)),
            client.watchAllOperations(e => this.onOperationChangeReceived(e, this.databaseNotifications, db)),
            client.watchAllNotificationUpdated(e => this.onNotificationUpdated(e, this.databaseNotifications)),
            client.watchAllDatabaseStatsChanged(e => collectionsTracker.default.onDatabaseStatsChanged(e))
        ];
    }

    databaseDisconnected() {
        this.clearTrackedOperationsForDatabase(this.currentDatabaseName);
        this.currentDatabaseName = null;
        this.databaseNotifications.removeAll();
    }

    monitorAttachmentUpload(notification: attachmentUpload) {
        this.databaseNotifications.push(notification);
    }
    
    private onRecentError(error: recentError) {
        if (error instanceof recentLicenseLimitError) {
            this.openDetails(error);
        }
        
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
            
            if (alertObject.alertReason() === "LicenseManager_LicenseLimit") {
                this.openDetails(alertObject);
            }
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {
        this.trackRecentOperation(database, operationDto);

        const spinnersCleanup = () => {
            if (operationDto.State.Status !== "InProgress") {
                // since kill request doesn't wait for actual kill, let's remove completed items
                this.spinners.kill.remove(operationDto.Id);
            }
        };
        
        const invokeOnChange = () => this.getOperationsWatch(database).onOperationChange(operationDto);
        
        for (let i = 0; i < this.customOperationHandler.length; i++) {
            const handler = this.customOperationHandler[i];
            if (handler.tryHandle(operationDto, notificationsContainer, database, {
                onChange: invokeOnChange,
                spinnersCleanup: spinnersCleanup
            })) {
                this.tryOpenPendingOperationDetails(database, operationDto.OperationId, operationDto);
                // low-level handler processed this notification  - we assume we are done
                return;
            }
        }
        
        const existingOperation = notificationsContainer().find(x => x.id === operationDto.Id) as operation;
        if (existingOperation) {
            let foundCustomMerger = false;
            for (let i = 0; i < this.customOperationMerger.length; i++) {
                const merger = this.customOperationMerger[i];
                if (merger.merge(existingOperation, operationDto)) {
                    foundCustomMerger = true;
                    existingOperation.invokeOnUpdateHandlers();
                    break;
                }
            }

            if (!foundCustomMerger) {
                existingOperation.updateWith(operationDto);
                existingOperation.invokeOnUpdateHandlers();
            }
        } else {
            const operationChangedObject = new operation(database, operationDto);

            // allow custom callbacks for mergers, passing undefined to distinguish between update and create.
            this.customOperationMerger.forEach(merger => {
                merger.merge(operationChangedObject, undefined);
            });
            
            operationChangedObject.invokeOnUpdateHandlers();

            notificationsContainer.push(operationChangedObject);
        }

        spinnersCleanup();
        invokeOnChange();
        this.tryOpenPendingOperationDetails(database, operationDto.OperationId, operationDto);
    }

    private onNotificationUpdated(notificationUpdatedDto: Raven.Server.NotificationCenter.Notifications.NotificationUpdated, notificationsContainer: KnockoutObservableArray<abstractNotification>) {

        const existingOperation = notificationsContainer().find(x => x.id === notificationUpdatedDto.NotificationId) as operation;
        if (existingOperation) {
            this.clearTrackedOperation(existingOperation.databaseName, existingOperation.operationId());
            this.removeNotificationFromNotificationCenter(existingOperation);
        }
    }

    private getOperationsWatch(db: database | string) {
        return db ? this.databaseOperationsWatch : this.globalOperationsWatch;
    }

    monitorOperation<T = unknown>(db: database | string,
        operationId: number,
        onProgress: (progress: T) => void = null): JQueryPromise<T> {

        return this.getOperationsWatch(db).monitorOperation(operationId, onProgress);
    }

    postpone(notification: abstractNotification, timeInSeconds: number): JQueryPromise<void> {
        const notificationId = notification.id;

        this.spinners.postpone.push(notificationId);

        return new postponeNotificationCommand(notification.databaseName, notificationId, timeInSeconds)
            .execute()
            .always(() => this.spinners.postpone.remove(notificationId))
            .done(() => this.removeNotificationFromNotificationCenter(notification));
    }

    dismissAll() {
        this.allNotifications().forEach(notification => this.dismiss(notification));
        this.isShowingAllNotifications(false);
    }

    dismiss(notification: abstractNotification) {
        if (!notification.canBeDismissed()) {
            return;
        }
        
        if (notification.requiresRemoteDismiss()) {
            const notificationId = notification.id;

            const shouldDismissForever = notification instanceof performanceHint && notification.dontShowAgain();

            this.spinners.dismiss.push(notificationId);

            new dismissNotificationCommand(notification.databaseName, notificationId, shouldDismissForever)
                .execute()
                .always(() => this.spinners.dismiss.remove(notificationId))
                .done(() => this.removeNotificationFromNotificationCenter(notification));
        } else {
            this.removeNotificationFromNotificationCenter(notification);
        }
    }

    private removeNotificationFromNotificationCenter(notification: abstractNotification) {
        if (notification instanceof operation) {
            this.clearTrackedOperation(notification.databaseName, notification.operationId());
        }

        this.globalNotifications.remove(notification);
        this.databaseNotifications.remove(notification);
    }

    killAttachmentUpload(upload: attachmentUpload) {
        return viewHelpers.confirmationMessage("Are you sure?", "Do you want to abort attachment upload?", {
            forceRejectWithResolve: true
        })
            .done((result: confirmDialogResult) => {
                if (result.can) {
                    // no need for spinners here - it is sync call
                    upload.abortUpload();
                    
                    this.databaseNotifications.remove(upload);
                }
            });
    }
    
    killOperation(operationToKill: operation) {
        return viewHelpers.confirmationMessage("Are you sure?", "Do you want to abort current operation?", {
            forceRejectWithResolve: true
        })
            .done((result: confirmDialogResult) => {
                if (result.can) {
                    const notificationId = operationToKill.id;

                    this.spinners.kill.push(notificationId);

                    new killOperationCommand(operationToKill.databaseName, operationToKill.operationId())
                        .execute()
                        .fail(() => {
                            // we don't call remove in always since killOperationCommand only delivers kill signal and doesn't wait for actual kill
                            this.spinners.kill.remove(notificationId);
                        });
                }
            });
    }

    openDetailsForOperationById(db: database | string, operationId: number): void {
        if (this.tryOpenOperationDetails(db, operationId)) {
            return;
        }

        this.pendingOperationDetails.add(this.getOperationTrackingKey(db, operationId));
    }

    private tryOpenPendingOperationDetails(
        db: database | string,
        operationId: number,
        operationDto?: Raven.Server.NotificationCenter.Notifications.OperationChanged
    ): boolean {
        const operationKey = this.getOperationTrackingKey(db, operationId);
        if (!this.pendingOperationDetails.has(operationKey)) {
            return false;
        }

        return this.tryOpenOperationDetails(db, operationId, operationDto);
    }

    private createOperationFromDto(
        db: database | string,
        operationDto?: Raven.Server.NotificationCenter.Notifications.OperationChanged
    ): operation {
        if (!operationDto) {
            return null;
        }

        return new operation(db, operationDto);
    }

    private getTopLevelOperationById(db: database | string, operationId: number): operation {
        const notificationsArray = db ? this.databaseNotifications() : this.globalNotifications();
        return notificationsArray.find(x => x instanceof operation && x.operationId() === operationId) as operation;
    }

    private getRecentOperationById(db: database | string, operationId: number): operation {
        const operationKey = this.getOperationTrackingKey(db, operationId);
        const operationDto = this.recentOperations.get(operationKey);
        return this.createOperationFromDto(db, operationDto);
    }

    private getGroupedOperationById(db: database | string, operationId: number): abstractNotification {
        const notificationsArray = db ? this.databaseNotifications() : this.globalNotifications();

        for (const groupedNotification of notificationsArray) {
            if (groupedNotification instanceof groupedVirtualNotification) {
                const match = groupedNotification.operations().find(x => x.operationId === operationId);
                if (match) {
                    return groupedNotification;
                }
            }
        }

        return null;
    }

    private tryOpenOperationDetails(
        db: database | string,
        operationId: number,
        operationDto?: Raven.Server.NotificationCenter.Notifications.OperationChanged
    ): boolean {
        const notification = this.resolveOperationDetailsTarget(db, operationId, operationDto);
        if (!notification) {
            return false;
        }

        this.clearTrackedOperation(db, operationId);
        this.openDetails(notification);
        return true;
    }

    private getOperationTrackingKey(db: database | string, operationId: number): string {
        const dbName = typeof db === "string" ? db : db?.name;
        const keyPrefix = dbName || "__serverWide__";
        return keyPrefix + "|" + operationId;
    }

    private resolveOperationDetailsTarget(
        db: database | string,
        operationId: number,
        operationDto?: Raven.Server.NotificationCenter.Notifications.OperationChanged
    ): abstractNotification {
        return this.getTopLevelOperationById(db, operationId)
            || this.createOperationFromDto(db, operationDto)
            || this.getRecentOperationById(db, operationId)
            || this.getGroupedOperationById(db, operationId);
    }

    private trackRecentOperation(
        db: database | string,
        operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged
    ) {
        const operationKey = this.getOperationTrackingKey(db, operationDto.OperationId);
        this.recentOperations.delete(operationKey);
        this.recentOperations.set(operationKey, operationDto);

        if (this.recentOperations.size > notificationCenter.maxRecentOperations) {
            const oldestTrackedOperation = this.recentOperations.keys().next().value;
            this.recentOperations.delete(oldestTrackedOperation);
        }
    }

    private clearTrackedOperation(db: database | string, operationId: number) {
        const operationKey = this.getOperationTrackingKey(db, operationId);
        this.pendingOperationDetails.delete(operationKey);
        this.recentOperations.delete(operationKey);
    }

    private clearTrackedOperationsForDatabase(databaseName: string) {
        if (!databaseName) {
            return;
        }

        const databasePrefix = databaseName + "|";
        const pendingOperationsToClear = Array.from(this.pendingOperationDetails).filter((key) => key.startsWith(databasePrefix));
        pendingOperationsToClear.forEach((key) => this.pendingOperationDetails.delete(key));

        const recentOperationsToClear = Array.from(this.recentOperations.keys()).filter((key) => key.startsWith(databasePrefix));
        recentOperationsToClear.forEach((key) => this.recentOperations.delete(key));
    }

    openDetails(notification: abstractNotification) {
        for (let i = 0; i < this.detailsProviders.length; i++) {
            const provider = this.detailsProviders[i];
            if (provider.supportsDetailsFor(notification)) {
                provider.showDetailsFor(notification, this);
                return;
            }
        }

        throw new Error("Unsupported notification: " + notification.type);
    }

    private shouldConsumeHideEvent(e: Event) {
        if (!this.pinNotifications()) {
            return $(e.target).closest(".notification-center-container").length === 0
                && $(e.target).closest("#notification-toggle").length === 0
                && $(e.target).closest(".modal.in").length === 0;
        }
    }

    filterBySeverity(severity: Raven.Server.NotificationCenter.Notifications.NotificationSeverity) {
        this.severityFilter(severity);
    }

    showAllNotifications() {
        this.isShowingAllNotifications(true);
    }
}

export = notificationCenter;
