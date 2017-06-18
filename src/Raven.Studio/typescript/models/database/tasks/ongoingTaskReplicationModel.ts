/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import generalUtils = require("common/generalUtils");

class ongoingTaskReplicationModel extends ongoingTask {

    editUrl: KnockoutComputed<string>; 

    apiKey = ko.observable<string>();
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
  
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Server.Web.System.OngoingTaskReplication) {
        super();
        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editExternalReplication(this.taskId); 
    }

    update(dto: Raven.Server.Web.System.OngoingTaskReplication) {
        super.update(dto);
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toDto(): externalReplicationDataFromUI {
        return {
            DestinationURL: this.destinationURL(),
            DestinationDB: this.destinationDB(),
            ApiKey: this.apiKey()
        };
    }

    private initValidation() {

        this.destinationDB.extend({
            required: true,
            maxLength: 230,
            validDatabaseName: true
        });

        this.destinationURL.extend({
            required: true,
            validUrl: true
        });

        this.apiKey.extend({
            required: false,
            validApiKey: true
        });

        this.validationGroup = ko.validatedObservable({
            destinationDB: this.destinationDB,
            destinationURL: this.destinationURL,
            apiKey: this.apiKey
        });
    }

    static empty(): ongoingTaskReplicationModel {

        return new ongoingTaskReplicationModel({
            TaskType: "Replication",
            DestinationDatabase: null,
            DestinationUrl: clusterTopologyManager.default.localNodeUrl()
        } as Raven.Server.Web.System.OngoingTaskReplication);
    }
}

export = ongoingTaskReplicationModel;
