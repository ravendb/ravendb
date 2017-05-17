/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTask = require("models/database/tasks/ongoingTask"); 

class ongoingTaskReplication extends  ongoingTask {
    
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    destDBText: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Web.System.OngoingTaskReplication) {
        super(dto);
        this.initializeObservables();
        this.update(dto);
    }
    
    initializeObservables() {
        super.initializeObservables();
        
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
        alert("edit task replication");
        // ...
    }

    removeTask() {
        alert("remove task replication");
        // ...
    }
}

export = ongoingTaskReplication;
