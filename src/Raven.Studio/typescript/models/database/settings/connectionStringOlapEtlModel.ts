/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import jsonUtil = require("common/jsonUtil");
import s3Settings = require("viewmodels/database/tasks/destinations/s3Settings");
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import getBackupLocationCommand = require("commands/database/tasks/getBackupLocationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import database = require("models/resources/database");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import azureSettings = require("viewmodels/database/tasks/destinations/azureSettings");
import googleCloudSettings = require("viewmodels/database/tasks/destinations/googleCloudSettings");
import glacierSettings = require("viewmodels/database/tasks/destinations/glacierSettings");
import ftpSettings = require("viewmodels/database/tasks/destinations/ftpSettings");

class connectionStringOlapEtlModel extends connectionStringModel {

    s3Settings = ko.observable<s3Settings>();
    localSettings = ko.observable<localSettings>();
    azureSettings = ko.observable<azureSettings>();
    googleCloudSettings = ko.observable<googleCloudSettings>();
    glacierSettings = ko.observable<glacierSettings>();
    ftpSettings = ko.observable<ftpSettings>();
    
    destinationsChecked: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;

    locationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    folderPathOptions = ko.observableArray<string>([]);

    allowedAwsRegions: Array<string>;

    spinners = {
        locationInfoLoading: ko.observable<boolean>(false)
    };
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString, 
                isNew: boolean, 
                tasks: { taskName: string; taskId: number }[],
                allowedAwsRegions: Array<string>) {
        super(isNew, tasks);
        
        this.allowedAwsRegions = allowedAwsRegions;
        this.update(dto);
        
        this.initObservables();
        this.initValidation();
    }

    update(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name);
        this.s3Settings(dto.S3Settings ? new s3Settings(dto.S3Settings, null, "OLAP") : s3Settings.empty(null, "OLAP"));
        this.localSettings(dto.LocalSettings ? new localSettings(dto.LocalSettings, "connectionString") : localSettings.empty("connectionString"));
        this.azureSettings(dto.AzureSettings ? new azureSettings(dto.AzureSettings, "OLAP") : azureSettings.empty("OLAP"));
        this.googleCloudSettings(dto.GoogleCloudSettings ? new googleCloudSettings(dto.GoogleCloudSettings, "OLAP") : googleCloudSettings.empty("OLAP"));
        this.glacierSettings(dto.GlacierSettings ? new glacierSettings(dto.GlacierSettings, this.allowedAwsRegions, "OLAP") : glacierSettings.empty(this.allowedAwsRegions, "OLAP"));
        this.ftpSettings(dto.FtpSettings ? new ftpSettings(dto.FtpSettings, "OLAP") : ftpSettings.empty("OLAP"));
    }
    
    initObservables() {
        const folderPath = this.localSettings().folderPath();
        if (folderPath) {
            this.updateLocationInfo(folderPath);
        }

        this.updateFolderPathOptions(folderPath);

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.s3Settings().dirtyFlag().isDirty,
            this.localSettings().dirtyFlag().isDirty,
            this.azureSettings().dirtyFlag().isDirty,
            this.googleCloudSettings().dirtyFlag().isDirty,
            this.glacierSettings().dirtyFlag().isDirty,
            this.ftpSettings().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.localSettings().folderPath.throttle(300).subscribe((newPathValue) => {
            if (this.localSettings().folderPath.isValid()) {
                this.updateLocationInfo(newPathValue);
                this.updateFolderPathOptions(newPathValue);
            } else {
                this.locationInfo([]);
                this.folderPathOptions([]);
                this.spinners.locationInfoLoading(false);
            }
        });

        this.destinationsChecked = ko.pureComputed(() => {
            const localEnabled = this.localSettings().enabled();
            const s3Enabled = this.s3Settings().enabled();
            const azureEnabled = this.azureSettings().enabled();
            const googleCloudEnabled = this.googleCloudSettings().enabled();
            const glacierEnabled = this.glacierSettings().enabled();
            const ftpEnabled = this.ftpSettings().enabled();
            
            return localEnabled || s3Enabled || azureEnabled || googleCloudEnabled || glacierEnabled || ftpEnabled;
        });
    }

    initValidation() {
        super.initValidation();
        
        this.destinationsChecked.extend({
            validation: [
                {
                    validator: () => this.destinationsChecked(),
                    message: "Please select at least one destination"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            destinationsChecked: this.destinationsChecked
        });
    }
    
    static empty(): connectionStringOlapEtlModel { 
        return new connectionStringOlapEtlModel({
            Type: "Olap",
            Name: "",
            S3Settings: s3Settings.empty(null, "OLAP").toDto(),
            LocalSettings: localSettings.empty("connectionString").toDto(),
            AzureSettings: azureSettings.empty("OLAP").toDto(),
            GoogleCloudSettings: googleCloudSettings.empty("OLAP").toDto(),
            GlacierSettings: glacierSettings.empty(null, "OLAP").toDto(),
            FtpSettings: ftpSettings.empty("OLAP").toDto()
        } , true, [], null);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString {
        return {
            Type: "Olap",
            Name: this.connectionStringName(),
            S3Settings: this.s3Settings().enabled() ? this.s3Settings().toDto() : undefined,
            LocalSettings: this.localSettings().enabled() ? this.localSettings().toDto() : undefined,
            AzureSettings: this.azureSettings().enabled() ? this.azureSettings().toDto() : undefined,
            GoogleCloudSettings: this.googleCloudSettings().enabled() ? this.googleCloudSettings().toDto() : undefined,
            GlacierSettings: this.glacierSettings().enabled() ? this.glacierSettings().toDto() : undefined,
            FtpSettings: this.ftpSettings().enabled() ? this.ftpSettings().toDto() : undefined
        };
    }

    private updateLocationInfo(path: string) {
        const getLocationCommand = new getBackupLocationCommand(path, activeDatabaseTracker.default.database());

        const getLocationTask = getLocationCommand
            .execute()
            .done((result: Raven.Server.Web.Studio.DataDirectoryResult) => {
                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.locationInfo(result.List);
            });

        generalUtils.delayedSpinner(this.spinners.locationInfoLoading, getLocationTask);
    }

    private updateFolderPathOptions(path: string) {
        getFolderPathOptionsCommand.forServerLocal(path, true, activeDatabaseTracker.default.database())
            .execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => {
                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.folderPathOptions(result.List);
            });
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringOlapEtlModel;
