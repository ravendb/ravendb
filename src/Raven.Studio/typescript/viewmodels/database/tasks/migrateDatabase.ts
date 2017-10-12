import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getRemoteServerVersion = require("commands/database/studio/getRemoteServerVersion");

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();
    
    spinners = {
        versionDetect: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        
        this.bindToCurrentInstance("detectServerVersion");
    }
    
    attached() {
        super.attached();

        this.updateHelpLink("YD9M1R"); //TODO: this is probably stale!
    }

    detectServerVersion() {
        if (!this.isValid(this.model.versionCheckValidationGroup)) {
            return;
        }
        this.spinners.versionDetect(true);
        
        new getRemoteServerVersion(this.model.serverUrl())
            .execute()
            .done(buildInfo => {
                if (buildInfo.MajorVersion !== "Unknown") {
                    this.model.serverMajorVersion(buildInfo.MajorVersion);
                }
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
