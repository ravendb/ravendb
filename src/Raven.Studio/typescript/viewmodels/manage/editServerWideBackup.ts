import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import serverWideBackupEditModel = require("models/database/tasks/serverWide/serverWideBackupEditModel");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");
import getServerWideBackupConfigCommand = require("commands/serverWide/tasks/getServerWideBackupConfigCommand");
import getServerWideTaskInfoCommand = require("commands/serverWide/tasks/getServerWideTaskInfoCommand");
import popoverUtils = require("common/popoverUtils");
import eventsCollector = require("common/eventsCollector");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import cronEditor = require("viewmodels/common/cronEditor");
import saveServerWideBackupCommand = require("commands/serverWide/tasks/saveServerWideBackupCommand");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class editServerWideBackup extends viewModelBase {
    
    editedTask = ko.observable<serverWideBackupEditModel>();
    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>();
    
    fullBackupCronEditor = ko.observable<cronEditor>();
    incrementalBackupCronEditor = ko.observable<cronEditor>();

    isAddingNewBackupTask = ko.observable<boolean>(true);
    
    possibleMentors = ko.observableArray<string>([]);

    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    constructor() {
        super();
        this.bindToCurrentInstance("testCredentials");
    }
    
    activate(args: any) {
        super.activate(args);

        const database = activeDatabaseTracker.default.database();
        const dbName = ko.observable<string>(database ? database.name : null);
        this.possibleMentors(clusterTopologyManager.default.topology().nodes().map(x => x.tag()));
        
        const backupLoader = () => {
            const deferred = $.Deferred<void>();

            if (args && args.taskName) {
                // 1. Editing an existing task
                this.isAddingNewBackupTask(false);
               
                getServerWideTaskInfoCommand.forBackup(args.taskName)
                    .execute()
                    .done((result: Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>) => {
                        if (result.Results.length) {
                            const backupTask = result.Results[0];
                            if (this.serverConfiguration().LocalRootPath && backupTask.LocalSettings.FolderPath && backupTask.LocalSettings.FolderPath.startsWith(this.serverConfiguration().LocalRootPath)) {
                                backupTask.LocalSettings.FolderPath = backupTask.LocalSettings.FolderPath.substr(this.serverConfiguration().LocalRootPath.length);
                            }

                            this.editedTask(new serverWideBackupEditModel(dbName, backupTask, this.serverConfiguration(), false, true));
                            deferred.resolve();
                        } else {
                            deferred.reject();
                            router.navigate(appUrl.forServerWideTasks());
                        }
                    })
                    .fail(() => {
                        deferred.reject();
                        router.navigate(appUrl.forServerWideTasks());
                    });
            } else {
                // 2. Creating a new task
                this.isAddingNewBackupTask(true);
                
                this.editedTask(serverWideBackupEditModel.empty(dbName, this.serverConfiguration(), false, true));
                deferred.resolve();
            }

            return deferred
                .then(() => {
                    this.dirtyFlag = this.editedTask().serverWideDirtyFlag;

                    this.fullBackupCronEditor(new cronEditor(this.editedTask().fullBackupFrequency));
                    this.incrementalBackupCronEditor(new cronEditor(this.editedTask().incrementalBackupFrequency));
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

        popoverUtils.longWithHover($(".backup-info"),
            {
                content: tasksCommonContent.generalBackupInfo
            });

        popoverUtils.longWithHover($(".backup-age-info"),
            {
                content: tasksCommonContent.backupAgeInfo
            });

        popoverUtils.longWithHover($(".bucket-info"),
            {
                content: tasksCommonContent.textForPopover("Bucket", "Server-Wide Backup") 
            });

        popoverUtils.longWithHover($(".bucket-gcs-info"),
            {
                content: tasksCommonContent.textForPopoverGCS("Bucket")
            });

        popoverUtils.longWithHover($(".storage-container-info"),
            {
                content: tasksCommonContent.textForPopover("Storage container", "Server-Wide Backup") 
            });

        popoverUtils.longWithHover($(".vault-info"),
            {
                content: tasksCommonContent.textForPopover("Vault", "Server-Wide Backup") 
            });

        popoverUtils.longWithHover($(".ftp-host-info"),
            {
                content: tasksCommonContent.ftpHostInfo
            });

        popoverUtils.longWithHover($(".serverwide-snapshot-encryption-info"),
            {
                content: tasksCommonContent.serverwideSnapshotEncryptionInfo
            });

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });
    }

    saveBackupSettings() {
        this.editedTask().encryptionSettings().setKeyUsedBeforeSave();
        
        if (!this.validate()) {
            return;
        }
        
        const dto = this.editedTask().toDto();

        if (this.serverConfiguration().LocalRootPath) {
            dto.LocalSettings.FolderPath = this.serverConfiguration().LocalRootPath + dto.LocalSettings.FolderPath;
        }

        this.spinners.save(true);
        eventsCollector.default.reportEvent("server-wide-backup", "save");
        
        new saveServerWideBackupCommand(dto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToServerWideTasksView();
            })
            .always(() => this.spinners.save(false));
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
        this.goToServerWideTasksView();
    }

    private goToServerWideTasksView() {
        router.navigate(appUrl.forServerWideTasks()); 
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedTask().validationGroup))
            valid = false;
        
        if (!this.isValid(this.editedTask().serverWideValidationGroup))
            valid = false;

        if (!this.isValid(this.editedTask().encryptionSettings().validationGroup()))
            valid = false;

        const localSettings = this.editedTask().localSettings();
        if (localSettings.enabled() && !this.isValid(localSettings.effectiveValidationGroup()))
            valid = false;

        const s3Settings = this.editedTask().s3Settings();
        if (s3Settings.enabled() && !this.isValid(s3Settings.effectiveValidationGroup()))
            valid = false;

        const azureSettings = this.editedTask().azureSettings();
        if (azureSettings.enabled() && !this.isValid(azureSettings.effectiveValidationGroup()))
            valid = false;

        const googleCloudSettings = this.editedTask().googleCloudSettings();
        if (googleCloudSettings.enabled() && !this.isValid(googleCloudSettings.effectiveValidationGroup()))
            valid = false;

        const glacierSettings = this.editedTask().glacierSettings();
        if (glacierSettings.enabled() && !this.isValid(glacierSettings.effectiveValidationGroup()))
            valid = false;

        const ftpSettings = this.editedTask().ftpSettings();
        if (ftpSettings.enabled() && !this.isValid(ftpSettings.effectiveValidationGroup()))
            valid = false;
        
        return valid;
    }
}

export = editServerWideBackup;
