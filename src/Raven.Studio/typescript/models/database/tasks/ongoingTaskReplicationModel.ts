/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskReplicationModel extends  ongoingTask {

    editUrl: KnockoutComputed<string>; 

    apiKey = ko.observable<string>();
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();

    destDBText: KnockoutComputed<string>; // just for ui..
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Server.Web.System.OngoingTaskReplication) {
        super(dto);
        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }
    
    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editExternalReplication(this.taskId.toString()); // ???
        
        this.destDBText = ko.pureComputed(() => {
            return `(${this.destinationDB()})`;
        });
    }

    update(dto: Raven.Server.Web.System.OngoingTaskReplication) {
        super.update(dto);
        this.destinationDB(dto.DestinationDB);
        this.destinationURL(dto.DestinationURL);
    }

    enableTask() {
        alert("enabling task replication");
        // ...
    }

    disableTask() {
        alert("disabling task replication");
        // ...
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    removeTask() {
        alert("remove task replication");

        // todo ... implement on the view model not on the model , pass data from html to view model !
    }

    private initValidation() {

        this.destinationDB.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => val !== null, // todo: validate db name format like in db creation !
                    message: "Please enter destination database name for replication"
                }]
        });

        this.destinationURL.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => val !== null,  // todo: validate url format !
                    message: "Please enter valid Url"
                }]
        });

        this.apiKey.extend({
            validation: [
                {
                    validator: (val: string) => val !== "1",  // todo: validate apiKey format ! I put 1 just for now... but can be null
                    message: "Please enter valid api key format"
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
            LastModificationTime: null,
            ResponsibleNode: null,        // todo: how to define & take from super ?
            TaskConnectionStatus:null,
            TaskState: null,
            TaskType: "Replication",
            DestinationDB: null,
            DestinationURL: null,
            TaskId: null
        });
    }

    static Simulation(): ongoingTaskReplicationModel {
        return new ongoingTaskReplicationModel({
            LastModificationTime: null,
            ResponsibleNode: null,        // todo: how to define & take from super ?
            TaskConnectionStatus: null,
            TaskState: null,
            TaskType: "Replication",
            DestinationDB: "simulationDB",
            DestinationURL: "http://localhost:8083",
            TaskId: null
        });
    }
}

export = ongoingTaskReplicationModel;
