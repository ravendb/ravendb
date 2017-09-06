/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskSqlEtlModel extends ongoingTask {
    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    destinationDatabaseText: KnockoutComputed<string>;

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtl, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
        this.initializeObservables();
        this.update(dto);
    }

    initializeObservables() {
        super.initializeObservables();
        // ...
        this.destinationDatabaseText = ko.pureComputed(() => {
            return `(${this.destinationDatabase()})`;
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtl) {
        super.update(dto);
        this.destinationServer(dto.DestinationServer);
        this.destinationDatabaseText(dto.DestinationDatabase);
    }

    editTask() {
        // TODO...
    }

    toggleDetails() {
        // TODO...
    }
}

export = ongoingTaskSqlEtlModel;
