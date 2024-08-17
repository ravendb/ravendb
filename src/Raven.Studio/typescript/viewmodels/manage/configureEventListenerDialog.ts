import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import licenseModel from "models/auth/licenseModel";
import awesomeMultiselect = require("common/awesomeMultiselect");
import genUtils = require("common/generalUtils");

type EventListenerConfigurationDto = Omit<
    Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration,
    "Persist"
>;

class configureEventListenerDialog extends dialogViewModelBase {
    view = require("views/manage/configureEventListenerDialog.html");

    canPersist = !licenseModel.cloudLicense();

    enabled = ko.observable<boolean>(false);
    eventTypes = ko.observableArray<Raven.Server.EventListener.EventType>([]);
    allocationsLoggingCount = ko.observable<number>();
    allocationsLoggingIntervalInMs = ko.observable<number>();
    minimumDurationInMs = ko.observable<number>();
    persist = ko.observable<boolean>(false);

    filterChangeTypes = ko.observable<boolean>();

    allEventTypes = genUtils.exhaustiveStringTuple<Raven.Server.EventListener.EventType>()(
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
    );

    validationGroup = ko.validatedObservable({
        allocationsLoggingCount: this.allocationsLoggingCount,
        allocationsLoggingIntervalInMs: this.allocationsLoggingIntervalInMs,
        minimumDurationInMs: this.minimumDurationInMs,
    });

    constructor(config: EventListenerConfigurationDto) {
        super();

        this.enabled(config.EventListenerMode === "ToLogFile");
        this.eventTypes(config.EventTypes || []);
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
            EventListenerMode: this.enabled() ? "ToLogFile" : "Off",
            EventTypes: this.eventTypes(),
            MinimumDurationInMs: this.minimumDurationInMs(),
            AllocationsLoggingIntervalInMs: this.allocationsLoggingIntervalInMs(),
            AllocationsLoggingCount: this.allocationsLoggingCount(),
        };

        dialog.close(this, result);
    }
}

export = configureEventListenerDialog;
