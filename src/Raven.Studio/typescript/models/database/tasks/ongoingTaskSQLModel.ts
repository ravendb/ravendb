/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskSQLModel extends ongoingTask {

    destinationServer = ko.observable<string>();
    destinationDatabase = ko.observable<string>();
    destinationDatabaseText: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Web.System.OngoingSqlEtl) {
        super();
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

    update(dto: Raven.Server.Web.System.OngoingSqlEtl) {
        super.update(dto);
        this.destinationServer(dto.DestinationServer);
        this.destinationDatabaseText(dto.DestinationDatabase);
    }

    enableTask() {
        alert("enabling task sql");
        // ...
    }

    disableTask() {
        alert("disabling task sql");
        // ...
    }

    editTask() {
        alert("edit task sql");
        // ...
    }

    removeTask() {
        alert("remove task sql");
        // ...
    }
}

export = ongoingTaskSQLModel;
