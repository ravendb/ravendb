import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import licenseModel from "models/auth/licenseModel";
import awesomeMultiselect = require("common/awesomeMultiselect");
import generalUtils = require("common/generalUtils");

type EventListenerConfigurationDto = Omit<
    Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration,
    "Persist"
>;

class configureEventListenerDialog extends dialogViewModelBase {
    view = require("views/manage/configureEventListenerDialog.html");

    canPersist = !licenseModel.cloudLicense();

    eventListenerMode = ko.observable<Raven.Server.EventListener.EventListenerMode>();
    eventTypes = ko.observableArray<Raven.Server.EventListener.EventType>([]);
    allocationsLoggingCount = ko.observable<number>();
    allocationsLoggingIntervalInMs = ko.observable<number>();
    minimumDurationInMs = ko.observable<number>();
    persist = ko.observable<boolean>(false);

    filterChangeTypes = ko.observable<boolean>();

    allEventTypes: Raven.Server.EventListener.EventType[] = [
        "Allocations",
        "Contention",
        "GC",
        "GCCreateConcurrentThread_V1",
        "GCFinalizers",
        "GCRestart",
        "GCSuspend",
        "ThreadCreated",
        "ThreadCreating",
        "ThreadPoolMinMaxThreads",
        "ThreadPoolWorkerThreadAdjustment",
        "ThreadPoolWorkerThreadAdjustmentSample",
        "ThreadPoolWorkerThreadAdjustmentStats",
        "ThreadPoolWorkerThreadStart",
        "ThreadPoolWorkerThreadStop",
        "ThreadPoolWorkerThreadWait",
        "ThreadRunning",
    ];

    validationGroup = ko.validatedObservable({
        allocationsLoggingCount: this.allocationsLoggingCount,
        allocationsLoggingIntervalInMs: this.allocationsLoggingIntervalInMs,
        minimumDurationInMs: this.minimumDurationInMs,
    });

    constructor(config: EventListenerConfigurationDto) {
        super();

        this.validatedServerData(config);

        this.eventTypes(config.EventTypes || []);
        this.eventListenerMode(config.EventListenerMode);
        this.allocationsLoggingCount(config.AllocationsLoggingCount);
        this.allocationsLoggingIntervalInMs(config.AllocationsLoggingIntervalInMs);
        this.minimumDurationInMs(config.MinimumDurationInMs);
        
        this.initValidation();
    }

    private initValidation() {
        this.allocationsLoggingCount.extend({ min: 0 });
        this.allocationsLoggingIntervalInMs.extend({ min: 0 });
        this.minimumDurationInMs.extend({ min: 0 });
    }

    private validatedServerData(config: EventListenerConfigurationDto) {
        // Checking if the EventTypes from the server matches the list of all possible eventType defined here
        const unexpectedEventTypes = config.EventTypes?.filter(x => !this.allEventTypes.includes(x)) || [];
        
        if (unexpectedEventTypes.length > 0) {
            throw new Error(`Unexpected event type(s): ${unexpectedEventTypes}`);
        }
    }

    formattedEventListenerMode(): string {
        const mode = this.eventListenerMode();
        
        switch (mode) {
            case "Off":
            case "None":
                return mode;
            case "ToLogFile":
                return "To Log File";
            default:
                return generalUtils.assertUnreachable(mode);
        }
    }

    attached() {
        awesomeMultiselect.build($("#eventTypes"), opts => {
            opts.includeSelectAllOption = true;
        });
    }

    close() {
        dialog.close(this);
    }

    save() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }

        const result: Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration = {
            Persist: this.persist(),
            EventListenerMode: this.eventListenerMode(),
            EventTypes: this.eventTypes(),
            MinimumDurationInMs: this.minimumDurationInMs(),
            AllocationsLoggingIntervalInMs: this.allocationsLoggingIntervalInMs(),
            AllocationsLoggingCount: this.allocationsLoggingCount(),
        };

        dialog.close(this, result);
    }
}

export = configureEventListenerDialog;
