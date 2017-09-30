import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplication>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
    private taskId: number = null;

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { 
        test: ko.observable<boolean>(false) 
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);

    shortErrorText: KnockoutObservable<string>;

    constructor() {
        super();
        this.bindToCurrentInstance("testConnection");
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            ongoingTaskInfoCommand.forExternalReplication(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.ServerWide.Operations.OngoingTaskReplication) => { 
                    this.editedExternalReplication(new ongoingTaskReplication(result, false));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        }
        else {
            // 2. Creating a new task
            this.isAddingNewReplicationTask(true);
            this.editedExternalReplication(ongoingTaskReplication.empty());
            deferred.resolve();
        }

        deferred.done(() => this.initObservables());
        return deferred;
    }

    private initObservables() {
        // Discard test connection result when url has changed
        this.editedExternalReplication().destinationURL.subscribe(() => this.testConnectionResult(null));

        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
    }

    compositionComplete() {
        document.getElementById('taskName').focus();
    }

    saveExternalReplication() {
        // 1. Validate model
        if (!this.validate()) {
             return;
        }

        // 2. Create/add the new replication task
        const dto = this.editedExternalReplication().toDto();

        this.taskId = this.isAddingNewReplicationTask() ? 0 : this.taskId;

        new saveExternalReplicationTaskCommand(this.activeDatabase(), this.taskId, dto)
            .execute()
            .done(() => {
                this.goToOngoingTasksView();
            });
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedExternalReplication().validationGroup))
            valid = false;

        return valid;
    }

    testConnection() {
        if (this.isValid(this.editedExternalReplication().destinationURL)) {
            eventsCollector.default.reportEvent("external-replication", "test-connection");

            this.spinners.test(true);

            new testClusterNodeConnectionCommand(this.editedExternalReplication().destinationURL())
                .execute()
                .done(result => this.testConnectionResult(result))
                .always(() => this.spinners.test(false));
        }
    }
}

export = editExternalReplicationTask;
