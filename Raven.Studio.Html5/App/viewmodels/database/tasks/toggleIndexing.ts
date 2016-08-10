import stopIndexingCommand = require("commands/database/index/stopIndexingCommand");
import startIndexingCommand = require("commands/database/index/startIndexingCommand");
import stopReducingCommand = require("commands/database/index/stopReducingCommand");
import startReducingCommand = require("commands/database/index/startReducingCommand");
import getIndexingStatusCommand = require("commands/database/index/getIndexingStatusCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");

class toggleIndexing extends viewModelBase {

    indexingStatus = ko.observable<any>();
    text = ko.observable();
    indexingStatusText: KnockoutComputed<string>;
    isMappingEnabled: KnockoutComputed<boolean>;
    isReducingEnabled: KnockoutComputed<boolean>;
    isIndexingEnabled: KnockoutComputed<boolean>;
    canDisableIndexing: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.isMappingEnabled = ko.computed(() => {
            var status = this.indexingStatus();
            if (!status) {
                return false;
            }

            return status.MappingStatus === "Mapping";
        });

        this.isReducingEnabled = ko.computed(() => {
            var status = this.indexingStatus();
            if (!status) {
                return false;
            }

            return status.ReducingStatus === "Reducing";
        });

        this.isIndexingEnabled = ko.computed(() => {
            var status = this.indexingStatus();
            var isMappingEnabled = this.isMappingEnabled();
            var isReducingEnabled = this.isReducingEnabled();

            if (!status) {
                return false;
            }

            return isMappingEnabled && isReducingEnabled;
        });

        this.canDisableIndexing = ko.computed(() => {
            var status = this.indexingStatus();
            var isMappingEnabled = this.isMappingEnabled();
            var isReducingEnabled = this.isReducingEnabled();

            if (!status) {
                return false;
            }

            return isMappingEnabled || isReducingEnabled;
        });

        this.indexingStatusText = ko.computed(() => {
            var status = this.indexingStatus();
            var isMappingEnabled = this.isMappingEnabled();
            var isReducingEnabled = this.isReducingEnabled();

            if (!status) {
                return "None";
            }

            if (isMappingEnabled && isReducingEnabled) {
                return "Mapping & Reducing";
            }

            if (isMappingEnabled) {
                return "Mapping only";
            }

            if (isReducingEnabled) {
                return "Reducing only";
            }

            if (status.MappingStatus === "Disabled" && status.ReducingStatus === "Disabled") {
                return "Disabled";
            }

            return "Paused";
        });
    }

    canActivate(args): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        this.getIndexStatus()
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forTasks(this.activeDatabase()) }));
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("VXOPAN");
    }

    disableIndexing() {
        eventsCollector.default.reportEvent("indexes", "disable-indexing");
        new stopIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    enableIndexing() {
        eventsCollector.default.reportEvent("indexes", "enable-indexing");
        new startIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    disableReducing() {
        eventsCollector.default.reportEvent("indexes", "disable-reducing");
        new stopReducingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    enableReducing() {
        eventsCollector.default.reportEvent("indexes", "enable-reducing");
        new startReducingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    getIndexStatus() {
        var deferred = $.Deferred();

        new getIndexingStatusCommand(this.activeDatabase())
            .execute()
            .done(result => {
                this.indexingStatus(result);
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }

}

export = toggleIndexing;
