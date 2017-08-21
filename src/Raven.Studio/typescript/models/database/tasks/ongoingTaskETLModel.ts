/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskETLModel extends ongoingTask {
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    destDBText: KnockoutComputed<string>;

    // Todo: Add support for the collections scripts dictionary

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl) {
        super();
        this.initializeObservables();
        this.update(dto);
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.destDBText = ko.pureComputed(() => {
            return `(${this.destinationDB()})`;
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl) {
        //TODO:
        super.update(dto);
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
    }

    editTask() {
        // TODO...
    }

    protected generateTaskName(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl): string {
        return `External replication to ${dto.DestinationDatabase}@${dto.DestinationUrl}`;
    }
}

export = ongoingTaskETLModel;
