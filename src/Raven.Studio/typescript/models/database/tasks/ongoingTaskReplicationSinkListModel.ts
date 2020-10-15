/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class ongoingTaskReplicationSinkListModel extends ongoingTaskListModel {
    
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<string>([]);
    hubDefinitionName = ko.observable<string>();

    connectionStringDefined: KnockoutComputed<boolean>;
    
    connectionStringsUrl: string;
  
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "raven", this.connectionStringName());
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editReplicationSink(this.taskId);
        this.connectionStringDefined = ko.pureComputed(() => !!this.destinationDB());
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || "N/A");
        this.connectionStringName(dto.ConnectionStringName);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls);
        this.hubDefinitionName(dto.HubDefinitionName);
    }

    toggleDetails() {
        this.showDetails.toggle();
    }

}

export = ongoingTaskReplicationSinkListModel;
