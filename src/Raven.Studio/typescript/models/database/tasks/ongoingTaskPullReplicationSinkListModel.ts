/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class ongoingTaskPullReplicationSinkListModel extends ongoingTaskListModel {
    editUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<string>([]);
    hubDefinitionName = ko.observable<string>();
    
    connectionStringsUrl: string; 
    
    showDetails = ko.observable(false);
  
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super();

        this.update(dto); 
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "raven", this.connectionStringName());
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editPullReplicationSink(this.taskId); 
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || 'N/A');
        this.connectionStringName(dto.ConnectionStringName);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls);
        this.hubDefinitionName(dto.HubDefinitionName);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle();
    }

}

export = ongoingTaskPullReplicationSinkListModel;
