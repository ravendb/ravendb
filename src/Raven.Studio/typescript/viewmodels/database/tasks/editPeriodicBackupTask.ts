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

class editPeriodicBackupTask extends viewModelBase {

    configuration = ko.observable<periodicBackupConfiguration>();
    isAddingNewBackupTask = ko.observable<boolean>(true);
    possibleMentors = ko.observableArray<string>([]);
    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>();

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
                        
                        this.configuration(new periodicBackupConfiguration(configuration, this.serverConfiguration()));
                        deferred.resolve();
                    })
                    .fail(() => {
                        deferred.reject();

                        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                    });
            } else {
                // 2. Creating a new task
                this.isAddingNewBackupTask(true);

                this.configuration(periodicBackupConfiguration.empty(this.serverConfiguration()));
                deferred.resolve();
            }

            deferred
                .done(() => {
                    this.dirtyFlag = this.configuration().dirtyFlag;
                });
            
            return deferred;
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
                            "<ul>" +
                                "<li>Backup includes documents, indexes and identities <br> " +
                                    "but doesn't include index data, indexes will be rebuilt after restore based on exported definitions</li>" +
                                "<li>Snapshot contains the raw data including the indexes - definitions and data</li>" +
                            "</ul>" +
                        "</li>" +
                        "<li>Speed" +
                            "<ul>" +
                                "<li>Backup is usually much faster than a Snapshot</li>" +
                            "</ul>" +
                        "</li>" +
                        "<li>Size" +
                            "<ul>" +
                                "<li>Backup is much smaller than Snapshot</li>" +
                            "</ul>" +
                        "</li>" +
                        "<li>Restore" +
                            "<ul>" +
                                "<li>Restore of a Snapshot is faster than of a Backup</li>" +
                            "</ul>" +
                        "</li>" +
                    "</ul>" +
                    "* An incremental Snapshot is the same as an incremental Backup"
            });

        popoverUtils.longWithHover($("#schedule-info"),
            {
                content: 
                    "<div class='schedule-info-text'>" +  
                    "Backup schedule is defined by a cron expression that can represent fixed times, dates, or intervals.<br/>" +
                    "We support cron expressions which consist of 5 <span style='color: #B9F4B7'>Fields</span>.<br/>" +
                    "Each field can contain any of the following <span style='color: #f9d291'>Values</span> along with " +
                    "various combinations of <span style='color: white'>Special Characters</span> for that field.<br/>" + 
                    "<pre>" +
                    "+----------------> minute (<span class='values'>0 - 59</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  +-------------> hour (<span class='values'>0 - 23</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  |  +----------> day of month (<span class='values'>1 - 31</span>) (<span class='special-characters'>, - * ? / L W</span>)<br/>" +
                    "|  |  |  +-------> month (<span class='values'>1-12 or JAN-DEC</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  |  |  |  +----> day of week (<span class='values'>0-6 or SUN-SAT</span>) (<span class='special-characters'>, - * ? / L #</span>)<br/>" +
                    "|  |  |  |  |<br/>" +
                    "<span style='color: #B9F4B7'>" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>" +
                    "</span></pre><br/>" +
                    "For more information see: <a href='http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html' target='_blank'>CronTrigger Tutorial</a></div>"
            });

        popoverUtils.longWithHover($("#bucket-info"),
            {
                content: this.textForPopover("Bucket")
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

        if (!this.isValid(this.configuration().localSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().s3Settings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().azureSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().glacierSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().ftpSettings().validationGroup))
            valid = false;

        return valid;
    }
}

export = editPeriodicBackupTask;
