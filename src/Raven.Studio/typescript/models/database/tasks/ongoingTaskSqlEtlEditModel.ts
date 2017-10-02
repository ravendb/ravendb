/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 

class ongoingTaskSqlEtlEditModel extends ongoingTaskEditModel {
    destinationServer = ko.observable<string>();  //TODO: check if needed
    destinationDatabase = ko.observable<string>(); //TODO: check if needed
    destinationDatabaseText: KnockoutComputed<string>; //TODO: check if needed

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlDetails) {
        super();

        this.isInTasksListView = true;
        this.initializeObservables();
        this.update(dto);
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.destinationDatabaseText = ko.pureComputed(() => {
            return `(${this.destinationDatabase()})`;
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlDetails) {
        super.update(dto);
        //TODO:
    }

    editTask() {
        // TODO...
    }

    toggleDetails() {
        // TODO...
    }
}

export = ongoingTaskSqlEtlEditModel;
