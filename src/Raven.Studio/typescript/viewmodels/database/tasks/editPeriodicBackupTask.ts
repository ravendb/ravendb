import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import getPeriodicBackupConfigurationCommand = require("commands/database/tasks/getPeriodicBackupConfigurationCommand");
import getPeriodicBackupConfigCommand = require("commands/database/tasks/getPeriodicBackupConfigCommand");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");
import popoverUtils = require("common/popoverUtils");
import eventsCollector = require("common/eventsCollector");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import cronEditor = require("viewmodels/common/cronEditor");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import manualBackupConfiguration = require("models/database/tasks/periodicBackup/manualBackupConfiguration");
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");

type backupConfigurationClass = manualBackupConfiguration | periodicBackupConfiguration;

class editPeriodicBackupTask extends viewModelBase {

    titleForView: KnockoutComputed<string>;
    configuration = ko.observable<backupConfigurationClass>();
    
    fullBackupCronEditor = ko.observable<cronEditor>();
    incrementalBackupCronEditor = ko.observable<cronEditor>();
    
    isAddingNewBackupTask = ko.observable<boolean>(true);
    possibleMentors = ko.observableArray<string>([]);
    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>();

    constructor() {
        super();
        
        this.bindToCurrentInstance("testCredentials");
        
        this.titleForView = ko.pureComputed(() => this.configuration().getTitleForView(this.isAddingNewBackupTask()));
    }

    activate(args: any) { 
        super.activate(args);

        const database = activeDatabaseTracker.default.database();
        const dbName = ko.observable<string>(database ? database.name : null);
        
        const backupLoader = () => {
            const deferred = $.Deferred<void>();

            if (args.taskId) {
                // 1. Editing an existing task
                this.isAddingNewBackupTask(false);
                new getPeriodicBackupConfigurationCommand(this.activeDatabase(), args.taskId)
                    .execute()
                    .done((configuration: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration) => {
                        if (this.serverConfiguration().LocalRootPath && configuration.LocalSettings.FolderPath && configuration.LocalSettings.FolderPath.startsWith(this.serverConfiguration().LocalRootPath)) {
                            configuration.LocalSettings.FolderPath = configuration.LocalSettings.FolderPath.substr(this.serverConfiguration().LocalRootPath.length);
                        }
                        
                        this.configuration(new periodicBackupConfiguration(dbName, configuration, this.serverConfiguration(), this.activeDatabase().isEncrypted(), false));
                        deferred.resolve();
                    })
                    .fail(() => {
                        deferred.reject();

                        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                    });
            } else if (args.manual) {
                    // 2. Create a one-time manual backup
                    this.configuration(manualBackupConfiguration.empty(dbName, this.serverConfiguration(), this.activeDatabase().isEncrypted()));
                    deferred.resolve();
            } else {
                    // 3. Add a new periodic backup task
                    this.isAddingNewBackupTask(true);
                    this.configuration(periodicBackupConfiguration.empty(dbName, this.serverConfiguration(), this.activeDatabase().isEncrypted(), false));
                    deferred.resolve();
            }

            return deferred
                .then(() => {
                    this.dirtyFlag = this.configuration().dirtyFlag;
                    this.fullBackupCronEditor(new cronEditor(this.configuration().getFullBackupFrequency()));
                    this.incrementalBackupCronEditor(new cronEditor(this.configuration().getIncrementalBackupFrequency()));
                });
        };

        return $.when<any>(this.loadPossibleMentors(), this.loadServerSideConfiguration())
            .then(backupLoader);
    }

    private loadServerSideConfiguration() {
        return new getPeriodicBackupConfigCommand(this.activeDatabase())
            .execute()
            .done(config => { 
                this.serverConfiguration(config);
            });
    }
    
    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
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

        this.setupDisableReasons();
        
        if (this.configuration() instanceof periodicBackupConfiguration) {
            document.getElementById("taskName").focus();
        }
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
                content: tasksCommonContent.textForPopover("Bucket")
            });

        popoverUtils.longWithHover($(".bucket-gcs-info"),
            {
                content: tasksCommonContent.textForPopoverGCS("Bucket")
            });

        popoverUtils.longWithHover($(".storage-container-info"),
            {
                content: tasksCommonContent.textForPopover("Storage container")
            });

        popoverUtils.longWithHover($(".vault-info"),
            {
                content: tasksCommonContent.textForPopover("Vault")
            });

        popoverUtils.longWithHover($(".ftp-host-info"),
            {
                content: tasksCommonContent.ftpHostInfo
            });

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });
    }

    onSubmit() {
        this.configuration().encryptionSettings().setKeyUsedBeforeSave();
        
        if (!this.validate()) {
             return;
        }
        
        eventsCollector.default.reportEvent("Backup", this.configuration().backupOperation);

        const dto = this.configuration().toDto();
        
        if (this.serverConfiguration().LocalRootPath) {
            dto.LocalSettings.FolderPath = this.serverConfiguration().LocalRootPath + dto.LocalSettings.FolderPath;
        }

        this.configuration().submit(this.activeDatabase(), dto).done(() => {
            this.dirtyFlag().reset();
            this.goToBackupsView();
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
        this.goToBackupsView();
    }

    private goToBackupsView() {
        router.navigate(appUrl.forBackups(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        const groupTocheck = this.configuration().validationGroup;
        
        if (!this.isValid(groupTocheck))
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

export = editPeriodicBackupTask;
