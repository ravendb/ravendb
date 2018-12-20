/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskSqlEtlListModel extends abstractOngoingTaskEtlListModel {
    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    connectionStringsUrl: string;
    
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
}

export = ongoingTaskSqlEtlListModel;
