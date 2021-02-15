import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");
import getServerWideBackupConfigCommand = require("commands/resources/getServerWideBackupConfigCommand");
import getServerWideBackupsCommand = require("commands/resources/getServerWideBackupCommand");
import popoverUtils = require("common/popoverUtils");
import eventsCollector = require("common/eventsCollector");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import cronEditor = require("viewmodels/common/cronEditor");
import saveServerWideBackupCommand = require("commands/resources/saveServerWideBackupCommand");
import backupCommonContent = require("models/database/tasks/periodicBackup/backupCommonContent");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class editServerWideBackup extends viewModelBase {
    
    // Using the periodic-backup-configuration-model in this view since there isn't much difference to account for a child class..
    configuration = ko.observable<periodicBackupConfiguration>();
    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>();
    
    fullBackupCronEditor = ko.observable<cronEditor>();
    incrementalBackupCronEditor = ko.observable<cronEditor>();

    isAddingNewBackupTask = ko.observable<boolean>(true);    
    
    constructor() {
        super();

        this.bindToCurrentInstance("testCredentials");
    }
    
    activate(args: any) {
        super.activate(args);

        const database = activeDatabaseTracker.default.database();
        const dbName = ko.observable<string>(database ? database.name : null);
        
        const backupLoader = () => {
            const deferred = $.Deferred<void>();

            if (args && args.taskName) { 
                // 1 Editing an existing task
                this.isAddingNewBackupTask(false);
               
                new getServerWideBackupsCommand(args.taskName)
                    .execute()
                    .done((backupsList: Raven.Server.Web.System.ServerWideBackupConfigurationResults) => {
                        if (backupsList.Results.length) {
                            const backupTask = backupsList.Results[0];
                            if (this.serverConfiguration().LocalRootPath && backupTask.LocalSettings.FolderPath && backupTask.LocalSettings.FolderPath.startsWith(this.serverConfiguration().LocalRootPath)) {
                                backupTask.LocalSettings.FolderPath = backupTask.LocalSettings.FolderPath.substr(this.serverConfiguration().LocalRootPath.length);
                            }

                            this.configuration(new periodicBackupConfiguration(dbName, backupTask, this.serverConfiguration(), false, true)); 
                            deferred.resolve();
                        } else {
                            deferred.reject();
                            router.navigate(appUrl.forServerWideBackupList());
                        }
                    })
                   .fail(() => {
                         deferred.reject();
                         router.navigate(appUrl.forServerWideBackupList()); 
                    });
            
            } else {
                // 2. Creating a new task
                this.isAddingNewBackupTask(true);
                
                this.configuration(periodicBackupConfiguration.empty(dbName, this.serverConfiguration(), false, true));
                deferred.resolve();
            }

            return deferred
                .then(() => {
                    this.dirtyFlag = this.configuration().dirtyFlag;

                    this.fullBackupCronEditor(new cronEditor(this.configuration().fullBackupFrequency));
                    this.incrementalBackupCronEditor(new cronEditor(this.configuration().incrementalBackupFrequency));
                });
        };

        return this.loadServerSideConfiguration()
            .then(backupLoader);
    }

    private loadServerSideConfiguration() {
        return new getServerWideBackupConfigCommand()
            .execute()
            .done(config => {
                this.serverConfiguration(config);
            });
    }  

    isBackupOptionAvailable(option: backupOptions) {
        const destinations = this.serverConfiguration().AllowedDestinations;
        if (destinations) {
            return _.includes(destinations, option);
        }
        return true;
    }

    compositionComplete() {
        super.compositionComplete();

        $('.edit-backup [data-toggle="tooltip"]').tooltip();

        $(".edit-backup .js-option-disabled").tooltip({
            title: "Destination was disabled by administrator",
            placement: "right"
        });
    }

    attached() {
        super.attached();

        popoverUtils.longWithHover($("#backup-info"),
            {
                content: backupCommonContent.generalBackupInfo
            });

        popoverUtils.longWithHover($("#backup-age-info"),
            {
                content: backupCommonContent.backupAgeInfo
            });

        popoverUtils.longWithHover($("#bucket-info"),
            {
                content: backupCommonContent.textForPopover("Bucket") 
            });

        popoverUtils.longWithHover($("#bucket-gcs-info"),
            {
                content: backupCommonContent.textForPopoverGCS("Bucket")
            });

        popoverUtils.longWithHover($("#storage-container-info"),
            {
                content: backupCommonContent.textForPopover("Storage container") 
            });

        popoverUtils.longWithHover($("#vault-info"),
            {
                content: backupCommonContent.textForPopover("Vault") 
            });

        popoverUtils.longWithHover($("#ftp-host-info"),
            {
                content: backupCommonContent.ftpHostInfo
            });

        popoverUtils.longWithHover($("#serverwide-snapshot-encryption-info"),
            {
                content: backupCommonContent.serverwideSnapshotEncryptionInfo
            });
    }

    saveBackupSettings() {
        this.configuration().encryptionSettings().setKeyUsedBeforeSave();
        
        if (!this.validate()) {
            return;
        }

        const dto = this.configuration().toDto();    

        if (this.serverConfiguration().LocalRootPath) {
            dto.LocalSettings.FolderPath = this.serverConfiguration().LocalRootPath + dto.LocalSettings.FolderPath;
        }

        eventsCollector.default.reportEvent("server-wide-backup", "save");
        
        new saveServerWideBackupCommand(dto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToServerWideBackupsView();
            });
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.effectiveValidationGroup())) {
            return;
        }

        bs.isTestingCredentials(true);
        bs.testConnectionResult(null);

        new testPeriodicBackupCredentialsCommand(bs.connectionType, bs.toDto())
            .execute()
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                bs.testConnectionResult(result);
            })
            .always(() => bs.isTestingCredentials(false));
    }

    cancelOperation() {
        this.goToServerWideBackupsView();
    }

    private goToServerWideBackupsView() {
        router.navigate(appUrl.forServerWideBackupList()); 
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.configuration().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().encryptionSettings().validationGroup()))
            valid = false;

        const localSettings = this.configuration().localSettings();
        if (localSettings.enabled() && !this.isValid(localSettings.effectiveValidationGroup()))
            valid = false;

        const s3Settings = this.configuration().s3Settings();
        if (s3Settings.enabled() && !this.isValid(s3Settings.effectiveValidationGroup()))
            valid = false;

        const azureSettings = this.configuration().azureSettings();
        if (azureSettings.enabled() && !this.isValid(azureSettings.effectiveValidationGroup()))
            valid = false;

        const googleCloudSettings = this.configuration().googleCloudSettings();
        if (googleCloudSettings.enabled() && !this.isValid(googleCloudSettings.effectiveValidationGroup()))
            valid = false;

        const glacierSettings = this.configuration().glacierSettings();
        if (glacierSettings.enabled() && !this.isValid(glacierSettings.effectiveValidationGroup()))
            valid = false;

        const ftpSettings = this.configuration().ftpSettings();
        if (ftpSettings.enabled() && !this.isValid(ftpSettings.effectiveValidationGroup()))
            valid = false;
        
        return valid;
    }
}

export = editServerWideBackup;
