/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTask = require("models/database/tasks/ongoingTask"); 

class ongoingTaskSQL extends ongoingTask {

    sqlProvider = ko.observable<string>();
    sqlTable = ko.observable<string>();
    sqlTableText: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Web.System.OngoingTaskSQL) {
        super(dto);
        this.initializeObservables();
        this.update(dto);
    }

    initializeObservables() {
        super.initializeObservables();
        // ...
        this.sqlTableText = ko.pureComputed(() => {
            return `(${this.sqlTable()})`;
        });
    }

    update(dto: Raven.Server.Web.System.OngoingTaskSQL) {
        super.update(dto);
        this.sqlProvider(dto.SqlProvider);
        this.sqlTable(dto.SqlTable);
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

export = ongoingTaskSQL;
