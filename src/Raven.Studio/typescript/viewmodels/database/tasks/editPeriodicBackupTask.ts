import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePeriodicBackupConfigurationCommand = require("commands/database/tasks/savePeriodicBackupConfigurationCommand");
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import getPeriodicBackupConfigurationCommand = require("commands/database/tasks/getPeriodicBackupConfigurationCommand");
import testPeriodicBackupCredentialsCommand = require("commands/database/tasks/testPeriodicBackupCredentialsCommand");
import popoverUtils = require("common/popoverUtils");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class editPeriodicBackupTask extends viewModelBase {

    configuration = ko.observable<periodicBackupConfiguration>();

    constructor() {
        super();
        
        this.bindToCurrentInstance("testCredentials");
    }

    activate(args: any) { 
        super.activate(args);
        
        const deferred = $.Deferred<void>();
        
        if (args.taskId) {
            // editing an existing task
            new getPeriodicBackupConfigurationCommand(this.activeDatabase(), args.taskId)
                .execute()
                .done((configuration: Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration) => {
                    this.configuration(new periodicBackupConfiguration(configuration));
                    deferred.resolve();
                })
                .fail(() => {
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                    deferred.reject();
                });
        }
        else {
            // creating a new task
            this.configuration(periodicBackupConfiguration.empty());
            deferred.resolve();
        }

        this.addS3Popover();

        return deferred;
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById("taskName").focus();
    }

    addS3Popover() {
        $("#s3-info").popover({
            html: true,
            trigger: 'hover',
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            content: 'Maps project the fields to search on or to group by. It uses LINQ query syntax.<br/>' +
                'Example:</br><pre><span class="token keyword">from</span> order <span class="token keyword">in</span>' +
                ' docs.Orders<br/><span class="token keyword">where</span> order.IsShipped<br/>' +
                '<span class="token keyword">select new</span><br/>{</br>   order.Date, <br/>   order.Amount,<br/>' +
                '   RegionId = order.Region.Id <br />}</pre>Each map function should project the same set of fields.'
        });
    }

    savePeriodicBackup() {
        if (!this.validate()) {
             return;
        }

        const dto = this.configuration().toDto();

        new savePeriodicBackupConfigurationCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => this.goToOngoingTasksView());
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.credentialsValidationGroup)) {
            return;
        }

        bs.isTestingCredentials(true);
        new testPeriodicBackupCredentialsCommand(this.activeDatabase(), bs.connectionType, bs.toDto())
            .execute()
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

        return valid;
    }
}

export = editPeriodicBackupTask;