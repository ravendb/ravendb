/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskReplicationModel extends ongoingTaskEditModel { //TODO: split into 2 classes
    editUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    showReplicationDetails = ko.observable(false);
  
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
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
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showReplicationDetails(!this.showReplicationDetails());
    }

    toDto(): externalReplicationDataFromUI {
        return {
            TaskName: this.taskName(),
            DestinationURL: this.destinationURL(),
            DestinationDB: this.destinationDB(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            //TODO: task id
        };
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

    static empty(): ongoingTaskReplicationModel {
        return new ongoingTaskReplicationModel({  
            TaskName: "",
            TaskType: "Replication",
            DestinationDatabase: null,
            DestinationUrl: clusterTopologyManager.default.localNodeUrl()
        } as Raven.Client.ServerWide.Operations.OngoingTaskReplication, false);
    }
}

export = ongoingTaskReplicationModel;
