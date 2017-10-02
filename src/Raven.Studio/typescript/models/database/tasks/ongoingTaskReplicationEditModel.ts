/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskReplicationEditModel extends ongoingTaskEditModel {

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    showReplicationDetails = ko.observable(false);
  
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super();

        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }
    
    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);
    }

    toDto(taskId: number): Raven.Client.ServerWide.ExternalReplication {
        return {
            Name: this.taskName(),
            Url: this.destinationURL(), 
            Database: this.destinationDB(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            TaskId: taskId
        } as Raven.Client.ServerWide.ExternalReplication;
    }

    initValidation() {

        this.initializeMentorValidation();
        
        this.destinationDB.extend({
            required: true,
            validDatabaseName: true
        });

        this.destinationURL.extend({
            required: true,
            validUrl: true
        });

        this.validationGroup = ko.validatedObservable({
            destinationDB: this.destinationDB,
            destinationURL: this.destinationURL,
            preferredMentor: this.preferredMentor
        });
    }

    static empty(): ongoingTaskReplicationEditModel {
        return new ongoingTaskReplicationEditModel({  
            TaskName: "",
            TaskType: "Replication",
            DestinationDatabase: null,
            DestinationUrl: clusterTopologyManager.default.localNodeUrl()
        } as Raven.Client.ServerWide.Operations.OngoingTaskReplication);
    }
}

export = ongoingTaskReplicationEditModel;
