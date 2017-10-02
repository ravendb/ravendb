/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskReplicationListModel extends ongoingTaskModel {
    editUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    showReplicationDetails = ko.observable(false);
  
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super();

        this.update(dto); 
        this.initializeObservables();
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editExternalReplication(this.taskId); 
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showReplicationDetails(!this.showReplicationDetails());
    }

}

export = ongoingTaskReplicationListModel;
