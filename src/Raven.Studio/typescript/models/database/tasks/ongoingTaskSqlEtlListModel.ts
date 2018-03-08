/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class ongoingTaskSqlEtlListModel extends ongoingTaskListModel {
    editUrl: KnockoutComputed<string>;  
       
    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    connectionStringsUrl: string;
    
    showDetails = ko.observable(false);

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();        

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "sql", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSqlEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView) {
        super.update(dto);

        this.destinationServer(dto.DestinationServer);
        this.destinationDatabase(dto.DestinationDatabase);
        this.connectionStringName(dto.ConnectionStringName);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle(); 
    }
}

export = ongoingTaskSqlEtlListModel;
