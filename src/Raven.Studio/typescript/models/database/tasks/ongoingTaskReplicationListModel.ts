/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");

class ongoingTaskReplicationListModel extends ongoingTaskListModel {
    editUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<string>([]);
    showDelayReplication = ko.observable<boolean>(false);
    delayReplicationTime = ko.observable<number>();
    delayHumane: KnockoutComputed<string>;

    connectionStringDefined: KnockoutComputed<boolean>;
    connectionStringsUrl: string; 
    
    showDetails = ko.observable(false);
  
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) {
        super();

        this.update(dto); 
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "raven", this.connectionStringName());
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.connectionStringDefined = ko.pureComputed(() => !!this.destinationDB());
        this.editUrl = urls.editExternalReplication(this.taskId); 
        this.delayHumane = ko.pureComputed(() => generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true));
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) {
        super.update(dto);

        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || 'N/A');
        this.connectionStringName(dto.ConnectionStringName);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls);
        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle();
    }

}

export = ongoingTaskReplicationListModel;
