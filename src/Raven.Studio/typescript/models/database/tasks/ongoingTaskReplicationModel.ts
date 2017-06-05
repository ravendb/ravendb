/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

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
            validation: [
                {
                    validator: (val: string) => val !== null, // TODO: validate db name format like in db creation !
                    message: "Please enter destination database name for replication"
                }]
        });

        this.destinationURL.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => val !== null,  // TODO: validate url format !
                    message: "Please enter valid Url"
                }]
        });

        this.apiKey.extend({
            validation: [
                {
                    validator: (val: string) => val !== "1",  // TODO: validate ApiKey format, as the server expects it to be ! I put '1' just for now... it can be null !
                    message: "Please enter valid Api key format"
                }]
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
            DestinationUrl: null
        } as Raven.Server.Web.System.OngoingTaskReplication);
    }

}

export = ongoingTaskReplicationModel;
