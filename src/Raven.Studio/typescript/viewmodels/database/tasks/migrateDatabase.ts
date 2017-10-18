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

        this.model.serverUrl.throttle(500).subscribe(this.detectServerVersion);
    }
    
    attached() {
        super.attached();

        this.updateHelpLink("YD9M1R"); //TODO: this is probably stale!
    }

    detectServerVersion() {

        this.afterAsyncValidationCompleted(this.model.versionCheckValidationGroup, () => {

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
                    }
                    else {
                        this.model.serverMajorVersion(null);
                    }
                }).fail(() => this.model.serverMajorVersion(null))
                .always(() => {
                    this.spinners.versionDetect(false);
                    if (url !== this.model.serverUrl())
                        this.detectServerVersion();
                });

        });
                
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
