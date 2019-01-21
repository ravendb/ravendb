import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePullReplicationHubTaskCommand = require("commands/database/tasks/savePullReplicationHubTaskCommand");
import pullReplicationDefinition = require("models/database/tasks/pullReplicationDefinition");
import eventsCollector = require("common/eventsCollector");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import jsonUtil = require("common/jsonUtil");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

class editPullReplicationHubTask extends viewModelBase {

    editedItem = ko.observable<pullReplicationDefinition>();
    isAddingNewTask = ko.observable<boolean>(true);
    private taskId: number = null;
    
    possibleMentors = ko.observableArray<string>([]);
    
    spinners = { 
        save: ko.observable<boolean>(false) 
    };

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewTask(false);
            this.taskId = args.taskId;

            getOngoingTaskInfoCommand.forPullReplicationHub(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections) => { 
                    this.editedItem(new pullReplicationDefinition(result.Definition));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewTask(true);
            this.editedItem(pullReplicationDefinition.empty());
            deferred.resolve();
        }

        deferred.done(() => this.initObservables());
        
        return $.when<any>(this.loadPossibleMentors(), deferred);
    }
    
    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }

    private initObservables() {        
        
        const model = this.editedItem();
        
        this.dirtyFlag = new ko.DirtyFlag([
            model.taskName,
            model.manualChooseMentor,
            model.preferredMentor,
            model.delayReplicationTime,
            model.showDelayReplication,
            //TODO: certs?
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus();
        
        $('.edit-pull-replication-hub-task [data-toggle="tooltip"]').tooltip(); 
    }

    savePullReplication() {
        if (!this.isValid(this.editedItem().validationGroup)) {
            return false;
        }

        this.spinners.save(true);

        const dto = this.editedItem().toDto(this.taskId);
        this.taskId = this.isAddingNewTask() ? 0 : this.taskId;

        eventsCollector.default.reportEvent("pull-replication-hub", "save");

        new savePullReplicationHubTaskCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            })
            .always(() => this.spinners.save(false));
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }
}

export = editPullReplicationHubTask;
