import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getRemoteServerVersion = require("commands/database/studio/getRemoteServerVersion");
import recentError = require("common/notifications/models/recentError");
import generalUtils = require("common/generalUtils");

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();
    
    spinners = {
        versionDetect: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        
        this.bindToCurrentInstance("detectServerVersion");

        const debouncedDetection = _.debounce(() => this.detectServerVersion(), 700);

        this.model.serverUrl.subscribe(() => {
            this.model.serverMajorVersion(null);
            debouncedDetection();
        });
    }
    
    attached() {
        super.attached();

        this.updateHelpLink("YD9M1R"); //TODO: this is probably stale!
    }

    detectServerVersion() {
        if (!this.isValid(this.model.versionCheckValidationGroup)) {
            this.model.serverMajorVersion(null);
            return;
        }
        
        this.spinners.versionDetect(true);

        const url = this.model.serverUrl();

        new getRemoteServerVersion(url)
            .execute()
            .done(buildInfo => {
                if (buildInfo.MajorVersion !== "Unknown") {
                    this.model.serverMajorVersion(buildInfo.MajorVersion);
                    this.model.buildVersion(buildInfo.BuildVersion);
                } else {
                    this.model.serverMajorVersion(null);
                    this.model.buildVersion(null);
                }
            })
            .fail((response: JQueryXHR) => {
                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                const message = generalUtils.trimMessage(messageAndOptionalException.message);
                this.model.serverMajorVersion.setError(message);
            })
            .always(() => this.spinners.versionDetect(false));
    }
    
    migrateDb() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "migrate");
        this.spinners.migration(true);

        const db = this.activeDatabase();

        new migrateDatabaseCommand(db, this.model)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(db, operationId);
            })
            .always(() => this.spinners.migration(false));
    }
}

export = migrateDatabase; 
