import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getMigratedServerUrlsCommand = require("commands/database/studio/getMigratedServerUrlsCommand");
import getRemoteServerVersionWithDatabasesCommand = require("commands/database/studio/getRemoteServerVersionWithDatabasesCommand");
import recentError = require("common/notifications/models/recentError");
import generalUtils = require("common/generalUtils");

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();

    spinners = {
        versionDetect: ko.observable<boolean>(false),
        getResourceNames: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        
        this.bindToCurrentInstance("detectServerVersion");

        const debouncedDetection = _.debounce((showVersionSpinner: boolean) => this.detectServerVersion(showVersionSpinner), 700);

        this.model.serverUrl.subscribe(() => {
            this.model.serverMajorVersion(null);
            debouncedDetection(true);
        });

        this.model.userName.subscribe(() => debouncedDetection(false));
        this.model.password.subscribe(() => debouncedDetection(false));
    }

    activate(args: any) {
        super.activate(args);

        const deferred = $.Deferred<void>();
        new getMigratedServerUrlsCommand(this.activeDatabase())
            .execute()
            .done(data => this.model.serverUrls(data.List))
            .always(() => deferred.resolve());

        return deferred;
    }

    attached() {
        super.attached();

        this.updateHelpLink("YD9M1R"); //TODO: this is probably stale!
    }

    detectServerVersion(showVersionSpinner: boolean) {
        if (!this.isValid(this.model.versionCheckValidationGroup)) {
            this.model.serverMajorVersion(null);
            return;
        }

        this.spinners.getResourceNames(true);
        if (showVersionSpinner) {
            this.spinners.versionDetect(true);
        }

        const url = this.model.serverUrl();
        new getRemoteServerVersionWithDatabasesCommand(url, this.model.userName(), this.model.password())
            .execute()
            .done(info => {
                if (info.MajorVersion !== "Unknown") {
                    this.model.serverMajorVersion(info.MajorVersion);
                    this.model.buildVersion(info.BuildVersion);
                    this.model.fullVersion(info.FullVersion);
                    this.model.databaseNames(info.DatabaseNames);
                    this.model.fileSystemNames(info.FileSystemNames);
                    this.model.authorized(info.Authorized);
                    if (!info.Authorized) {
                        this.model.resourceName.valueHasMutated();
                    }
                } else {
                    this.model.serverMajorVersion(null);
                    this.model.buildVersion(null);
                    this.model.fullVersion(null);
                    this.model.databaseNames([]);
                    this.model.fileSystemNames([]);
                    this.model.authorized(true);
                }
            })
            .fail((response: JQueryXHR) => {
                if (url === this.model.serverUrl()) {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    const message = generalUtils.trimMessage(messageAndOptionalException.message);
                    this.model.serverMajorVersion.setError(message);
                    this.model.databaseNames([]);
                    this.model.fileSystemNames([]);
                }
            })
            .always(() => {
                this.spinners.getResourceNames(false);
                if (showVersionSpinner) {
                    this.spinners.versionDetect(false);
                }
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
