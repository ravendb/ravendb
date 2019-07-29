import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePeriodicBackupConfigurationCommand = require("commands/database/tasks/savePeriodicBackupConfigurationCommand");
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import getPeriodicBackupConfigurationCommand = require("commands/database/tasks/getPeriodicBackupConfigurationCommand");
import getPeriodicBackupConfigCommand = require("commands/database/tasks/getPeriodicBackupConfigCommand");
import testPeriodicBackupCredentialsCommand = require("commands/database/tasks/testPeriodicBackupCredentialsCommand");
import popoverUtils = require("common/popoverUtils");
import eventsCollector = require("common/eventsCollector");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import cronEditor = require("viewmodels/common/cronEditor");

class editPeriodicBackupTask extends viewModelBase {

    configuration = ko.observable<periodicBackupConfiguration>();
    
    fullBackupCronEditor = ko.observable<cronEditor>();
    incrementalBackupCronEditor = ko.observable<cronEditor>();
    
    isAddingNewBackupTask = ko.observable<boolean>(true);
    possibleMentors = ko.observableArray<string>([]);
    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>();

    encryptionSection: setupEncryptionKey;

    constructor() {
        super();
        
        this.bindToCurrentInstance("testCredentials");
    }

    activate(args: any) { 
        super.activate(args);

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
                        
                        this.configuration(new periodicBackupConfiguration(configuration, this.serverConfiguration(), this.activeDatabase().isEncrypted()));
                        deferred.resolve();
                    })
                    .fail(() => {
                        deferred.reject();

                        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                    });
            } else {
                // 2. Creating a new task
                this.isAddingNewBackupTask(true);

                this.configuration(periodicBackupConfiguration.empty(this.serverConfiguration(), this.activeDatabase().isEncrypted()));
                deferred.resolve();
            }

            return deferred
                .then(() => {
                    const dbName = ko.pureComputed(() => {
                        const db = this.activeDatabase();
                        return db ? db.name : null;
                    });
                    const encryptionSettings = this.configuration().encryptionSettings();
                    this.encryptionSection = setupEncryptionKey.forBackup(encryptionSettings.key, encryptionSettings.keyConfirmation, dbName);
                    this.dirtyFlag = this.configuration().dirtyFlag;

                    this.fullBackupCronEditor(new cronEditor(this.configuration().fullBackupFrequency));
                    this.incrementalBackupCronEditor(new cronEditor(this.configuration().incrementalBackupFrequency));
                    
                    if (!encryptionSettings.key()) {
                        return this.encryptionSection.generateEncryptionKey()
                            .done(() => {
                                encryptionSettings.dirtyFlag().reset();
                            });
                    }
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

        this.encryptionSection.syncQrCode(); 
        
        this.configuration().encryptionSettings().key.subscribe(() => this.encryptionSection.syncQrCode());
        
        $('.edit-backup [data-toggle="tooltip"]').tooltip();
        
        $(".edit-backup .js-option-disabled").tooltip({
            title: "Destination was disabled by administrator",
            placement: "right"
        });
        
        document.getElementById("taskName").focus();
    }

    attached() {
        super.attached();
        
        popoverUtils.longWithHover($("#backup-info"),
            {
                content: 
                    "Differences between Backup and Snapshot:" +
                    "<ul>" +
                        "<li>Data" +
                            "<small><ul>" +
                                "<li><strong>Backup</strong> includes documents, indexes definitions and identities.<br> " +
                                    "It doesn't include the index data itself, the indexes will be rebuilt after Restore, based on exported definitions.</li>" +
                                "<li><strong>Snapshot</strong> contains the raw data including the indexes (definitions and data).</li>" +
                            "</ul></small>" +
                        "</li>" +
                        "<li>Speed" +
                            "<small><ul>" +
                                "<li><strong>Backup</strong> is usually much faster than a <strong>Snapshot</strong></li>" +
                            "</ul></small>" +
                        "</li>" +
                        "<li>Size" +
                            "<small><ul>" +
                                "<li><strong>Backup</strong> is much smaller than <strong>Snapshot</strong></li>" +
                            "</ul></small>" +
                        "</li>" +
                        "<li>Restore" +
                            "<small><ul>" +
                                "<li>Restore of a <strong>Snapshot</strong> is faster than of a <strong>Backup</strong></li>" +
                            "</ul></small>" +
                        "</li>" +
                    "</ul></>" +
                    "Note: An incremental Snapshot is the same as an incremental Backup"
            });

        popoverUtils.longWithHover($("#backup-age-info"),
            {
                content:
                    "<ul>" +
                    "<li>Define the minimum time to keep the Backups (and Snapshots) in the system.<br></li>" +
                    "<li>A <strong>Full Backup</strong> that is older than the specified retention time will be deleted by RavenDB server.<br>" +
                    "If <strong>Incremental Backups</strong> exists, the Full Backup, and its incrementals, are removed only if the <em>last incremental</em> is older than the defined retention time.<br></li>"+
                    "<li>The deletion occurs when the backup task is triggered on its schedule.</li>" +
                    "</ul>"
            });

        popoverUtils.longWithHover($("#bucket-info"),
            {
                content: this.textForPopover("Bucket")
            });

        popoverUtils.longWithHover($("#bucket-gcs-info"),
            {
                content: this.textForPopovergcs("Bucket")
            });

        popoverUtils.longWithHover($("#storage-container-info"),
            {
                content: this.textForPopover("Storage container")
            });

        popoverUtils.longWithHover($("#vault-info"),
            {
                content: this.textForPopover("Vault")   
            });

        popoverUtils.longWithHover($("#ftp-host-info"),
            {
                content:
                    "To specify the server protocol, prepend the host with protocol identifier (ftp and ftps are supported).<br />" +
                    "If no protocol is specified the default one (ftp://) will be used.<br />" +
                    "You can also enter a complete URL e.g. <strong>ftp://host.name:port/backup-folder/nested-backup-folder</strong>"
            });
    }

    private textForPopover(storageName: string): string {
        return `${storageName} should be created manually in order for this backup to work.<br /> ` +
            "You can use the 'Test credentials' button to verify its existance.";
    }

    private textForPopovergcs(storageName: string): string {
        return `${storageName} should be created manually in order for this backup to work.<br /> ` +
            "You can use the 'Test credentials' button to verify its existance.<br />" +
            "<a href='https://cloud.google.com/storage/docs/bucket-naming' target='_blank'>Bucket naming guidelines</a>";
    }

    savePeriodicBackup() {
        if (!this.validate()) {
             return;
        }

        const dto = this.configuration().toDto();
        
        if (this.serverConfiguration().LocalRootPath) {
            dto.LocalSettings.FolderPath = this.serverConfiguration().LocalRootPath + dto.LocalSettings.FolderPath;
        }

        eventsCollector.default.reportEvent("periodic-backup", "save");
        
        new savePeriodicBackupConfigurationCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            });
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.validationGroup)) {
            return;
        }

        bs.isTestingCredentials(true);
        bs.testConnectionResult(null);
        
        new testPeriodicBackupCredentialsCommand(this.activeDatabase(), bs.connectionType, bs.toDto())
            .execute()
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                bs.testConnectionResult(result);        
            })
            .always(() => bs.isTestingCredentials(false));
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.configuration().validationGroup))
            valid = false;
        
        if (!this.isValid(this.configuration().encryptionSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().localSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().s3Settings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().azureSettings().validationGroup))
            valid = false;
            
        if (!this.isValid(this.configuration().googleCloudSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().glacierSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().ftpSettings().validationGroup))
            valid = false;

        return valid;
    }
}

export = editPeriodicBackupTask;
