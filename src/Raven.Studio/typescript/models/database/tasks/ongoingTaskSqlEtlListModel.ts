/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class ongoingTaskSqlEtlListModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;  
       
    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    connectionStringsUrl: string;
    
    showSqlEtlDetails = ko.observable(false);

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();        

        this.connectionStringsUrl = `${appUrl.forCurrentDatabase().connectionStrings()}` + `&type=sql&name=${this.connectionStringName()}`;
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSqlEtl(this.taskId);
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlListView) {
        super.update(dto);

        this.destinationServer(dto.DestinationServer);
        this.destinationDatabase(dto.DestinationDatabase);
        this.connectionStringName(dto.ConnectionStringName);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showSqlEtlDetails.toggle(); 
    }
}

export = ongoingTaskSqlEtlListModel;
