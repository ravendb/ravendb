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
    hasRevisionsConfiguration: KnockoutComputed<boolean>;

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

        this.hasRevisionsConfiguration = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (!db) {
                return false;
            }

            return db.hasRevisionsConfiguration();
        });

        this.model.revisionsAreConfigured = ko.pureComputed(() => {
            return this.activeDatabase().hasRevisionsConfiguration();
        });
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

    detectServerVersion() {
        if (!this.isValid(this.model.versionCheckValidationGroup)) {
            this.model.serverMajorVersion(null);
            return;
        }
        
        this.spinners.versionDetect(true);

        const url = this.model.serverUrl();

        new getRemoteServerVersionWithDatabasesCommand(url)
            .execute()
            .done(info => {
                if (info.MajorVersion !== "Unknown") {
                    this.model.serverMajorVersion(info.MajorVersion);
                    this.model.buildVersion(info.BuildVersion);
                    this.model.databaseNames(info.DatabaseNames);
                } else {
                    this.model.serverMajorVersion(null);
                    this.model.buildVersion(null);
                    this.model.databaseNames([]);
                }
            })
            .fail((response: JQueryXHR) => {
                if (url === this.model.serverUrl()) {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    const message = generalUtils.trimMessage(messageAndOptionalException.message);
                    this.model.serverMajorVersion.setError(message);
                    this.model.databaseNames([]);
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
