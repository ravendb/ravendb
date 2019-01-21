/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import generalUtils = require("common/generalUtils");
import ongoingTaskPullReplicationHubListModel = require("models/database/tasks/ongoingTaskPullReplicationHubListModel");

class ongoingTaskPullReplicationHubDefinitionListModel {
    
    taskId: number;
    taskName = ko.observable<string>();
    taskState = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>();

    showDelayReplication = ko.observable<boolean>(false);
    delayReplicationTime = ko.observable<number>();
    delayHumane: KnockoutComputed<string>;
    
    ongoingHubs = ko.observableArray<ongoingTaskPullReplicationHubListModel>([]);

    editUrl: KnockoutComputed<string>;
    stateText: KnockoutComputed<string>;

    showDetails = ko.observable(false);
  
    constructor(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.update(dto);

        this.initObservables();
    }

    taskType(): Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType {
        return "PullReplicationAsHub";
    }

    editTask() {
        router.navigate(this.editUrl());
    }
    
    private initObservables() {
        this.stateText = ko.pureComputed(() => this.taskState());

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editPullReplicationHub(this.taskId);

        this.delayHumane = ko.pureComputed(() => generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true));
    }
    
    update(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        
        this.taskName(dto.Name);
        this.taskState(dto.Disabled ? "Disabled" : "Enabled");
        this.taskId = dto.TaskId;

        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);
    }

    updateChildren(ongoingTasks: Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub>) {
        const existingNames = this.ongoingHubs().map(x => x.uniqueName);
        
        ongoingTasks.forEach(incomingTask => {
           const uniqueName = ongoingTaskPullReplicationHubListModel.generateUniqueName(incomingTask); 
           const existingItem = this.ongoingHubs().find(x => x.uniqueName === uniqueName);
           if (existingItem) {
               existingItem.update(incomingTask);
               _.pull(existingNames, uniqueName);
           } else {
               this.ongoingHubs.push(new ongoingTaskPullReplicationHubListModel(incomingTask));
           }
        });
        
        existingNames.forEach(toDelete => {
            const item = this.ongoingHubs().find(x => x.uniqueName === toDelete);
            if (item) {
                this.ongoingHubs.remove(item);
            }
        });
    }

    toggleDetails() {
        this.showDetails.toggle();
    }

}

export = ongoingTaskPullReplicationHubDefinitionListModel;
