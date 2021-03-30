/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskSqlEtlListModel extends abstractOngoingTaskEtlListModel {
    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    
    connectionStringDefined = ko.observable<boolean>();
    destinationDescription: KnockoutComputed<string>;
    
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
        
        this.destinationDescription = ko.pureComputed(() => {
            if (this.connectionStringDefined()) {
                const server = this.destinationServer();
                const database = this.destinationDatabase();
                return (database || "") + "@" + (server || "");
            } 
            
            return null;
        })
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView) {
        super.update(dto);

        this.destinationServer(dto.DestinationServer);
        this.destinationDatabase(dto.DestinationDatabase);
        this.connectionStringName(dto.ConnectionStringName);
        this.connectionStringDefined(dto.ConnectionStringDefined);
    }
}

export = ongoingTaskSqlEtlListModel;
