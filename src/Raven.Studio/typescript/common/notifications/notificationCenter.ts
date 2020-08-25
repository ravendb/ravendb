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
import virtualBulkInsertDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualBulkInsertDetails");
import virtualUpdateByQueryDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualUpdateByQueryDetails");
import virtualDeleteByQueryDetails = require("viewmodels/common/notificationCenter/detailViewer/virtualOperations/virtualDeleteByQueryDetails");
import bulkInsertDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/bulkInsertDetails");
import revertRevisionsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/revertRevisionsDetails");
import enforceRevisionsConfigurationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/enforceRevisionsConfigurationDetails");
import replayTransactionCommandsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/replayTransactionCommandsDetails");
import deleteDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/deleteDocumentsDetails");
import generateClientCertificateDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/generateClientCertificateDetails");
import compactDatabaseDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/compactDatabaseDetails");
import indexingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/indexingDetails");
import slowSqlDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/slowSqlDetails");
import slowWriteDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/slowWriteDetails");
import pagingDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/pagingDetails");
import hugeDocumentsDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/hugeDocumentsDetails");
import newVersionAvailableDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/newVersionAvailableDetails");
import etlTransformOrLoadErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/etlTransformOrLoadErrorDetails");
import genericAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/genericAlertDetails");
import recentErrorDetails = require("viewmodels/common/notificationCenter/detailViewer/recentErrorDetails");
import notificationCenterSettings = require("common/notifications/notificationCenterSettings");
import licenseLimitDetails = require("viewmodels/common/notificationCenter/detailViewer/licenseLimitDetails");
import requestLatencyDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/requestLatencyDetails");
import transactionCommandsDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/transactionCommandsDetails");
import dumpRawIndexDataDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/dumpRawIndexDataDetails");

import studioSettings = require("common/settings/studioSettings");

interface detailsProvider {
    supportsDetailsFor(notification: abstractNotification): boolean;
    showDetailsFor(notification: abstractNotification, notificationCenter: notificationCenter): JQueryPromise<void> | void;
}

interface customOperationMerger {
    merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean;
}

interface customOperationHandler {
    tryHandle(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
              database: database, callbacks: { spinnersCleanup: Function, onChange: Function }): boolean;
}

class notificationCenter {
    static instance = new notificationCenter();

    static readonly postponeOptions = notificationCenterSettings.postponeOptions;

    spinners = {
        dismiss: ko.observableArray<string>([]),
        postpone: ko.observableArray<string>([]),
        kill: ko.observableArray<string>([])
    };

    showNotifications = ko.observable<boolean>(false);
    pinNotifications = ko.observable<boolean>(false);
    includeInDom = ko.observable<boolean>(false); // to avoid RavenDB-10660

    globalNotifications = ko.observableArray<abstractNotification>();
    databaseNotifications = ko.observableArray<abstractNotification>();

    globalOperationsWatch = new notificationCenterOperationsWatch();
    databaseOperationsWatch = new notificationCenterOperationsWatch();

    allNotifications: KnockoutComputed<abstractNotification[]>;
    visibleNotifications: KnockoutComputed<abstractNotification[]>;

    totalItemsCount: KnockoutComputed<number>;
    successItemsCount: KnockoutComputed<number>;
    infoItemsCount: KnockoutComputed<number>;
    warningItemsCount: KnockoutComputed<number>;
    errorItemsCount: KnockoutComputed<number>;

    alertCountAnimation = ko.observable<boolean>();
    noNewNotifications: KnockoutComputed<boolean>;

    severityFilter = ko.observable<Raven.Server.NotificationCenter.Notifications.NotificationSeverity>();

    detailsProviders = [] as Array<detailsProvider>;
    customOperationMerger = [] as Array<customOperationMerger>;
    customOperationHandler = [] as Array<customOperationHandler>;

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
            generateClientCertificateDetails,
            deleteDocumentsDetails,
            bulkInsertDetails,
            revertRevisionsDetails,
            enforceRevisionsConfigurationDetails,
            compactDatabaseDetails,
            replayTransactionCommandsDetails,
            transactionCommandsDetails,
            dumpRawIndexDataDetails,
            
            // virtual operations:
            virtualBulkInsertDetails,
            virtualUpdateByQueryDetails,
            virtualDeleteByQueryDetails,

            // performance hints:
            indexingDetails,
            slowSqlDetails,
            slowWriteDetails,
            pagingDetails,
            requestLatencyDetails,
            hugeDocumentsDetails,
            
            // alerts:
            newVersionAvailableDetails,
            etlTransformOrLoadErrorDetails,

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

            return _.sortBy(mergedNotifications, x => -1 * x.displayDate().unix());
        });

        this.visibleNotifications = ko.pureComputed(() => {
            const severity = this.severityFilter();
            const allNotifications = this.allNotifications();
            if (!severity) {
                return allNotifications;
            }

            return allNotifications.filter(x => x.severity() === severity);
        });

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
        $("#notification-center").on('transitionend', e => {
            if (!this.showNotifications()) {
                this.includeInDom(false);
            }
        });
        
        this.showNotifications.subscribe((show: boolean) => {
            if (show) {
                this.includeInDom(true);
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
            
            if (alertObject.alertType() === "LicenseManager_LicenseLimit") {
                this.openDetails(alertObject);
            }
        }
    }

    private onOperationChangeReceived(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
        database: database) {
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

    postpone(notification: abstractNotification, timeInSeconds: number): JQueryPromise<void> {
        const notificationId = notification.id;

        this.spinners.postpone.push(notificationId);

        return new postponeNotificationCommand(notification.database, notificationId, timeInSeconds)
            .execute()
            .always(() => this.spinners.postpone.remove(notificationId))
            .done(() => this.removeNotificationFromNotificationCenter(notification));
    }

    dismissAll() {
        this.allNotifications().forEach(notification => this.dismiss(notification));
    }

    dismiss(notification: abstractNotification) {
        if (!notification.canBeDismissed()) {
            return;
        }
        
        if (notification instanceof recentError) {
            // local dismiss
            this.globalNotifications.remove(notification);
        } else if (notification instanceof attachmentUpload) {
            // local dismiss
            this.databaseNotifications.remove(notification);
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

                    new killOperationCommand(operationToKill.database, operationToKill.operationId())
                        .execute()
                        .fail(() => {
                            // we don't call remove in always since killOperationCommand only delivers kill signal and doesn't wait for actual kill
                            this.spinners.kill.remove(notificationId);
                        });
                }
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
        for (let i = 0; i < this.detailsProviders.length; i++) {
            const provider = this.detailsProviders[i];
            if (provider.supportsDetailsFor(notification)) {
                provider.showDetailsFor(notification, this);
                return;
            }
        }

        throw new Error("Unsupported notification: " + notification);
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
}

export = notificationCenter;
