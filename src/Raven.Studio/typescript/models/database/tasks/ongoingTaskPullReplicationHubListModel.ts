/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 

// this class represents connection between current node (hub) and remote node (sink)
class ongoingTaskPullReplicationHubListModel extends ongoingTaskListModel {
    
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub) {
        super();

        this.update(dto); 
        this.initializeObservables();
    }
    
    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || 'N/A');
    }
    
    toggleDetails(): void {
        throw new Error("Use toggleDetails on pullReplicationHub definition level");
    }
}

export = ongoingTaskPullReplicationHubListModel;
