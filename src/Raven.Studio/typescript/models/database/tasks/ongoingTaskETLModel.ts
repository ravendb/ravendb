/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskETLModel extends ongoingTask {

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    destDBText: KnockoutComputed<string>;

    // Todo: Add support for the collections scripts dictionary

    constructor(dto: Raven.Server.Web.System.OngoingRavenEtl) {
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

    update(dto: Raven.Server.Web.System.OngoingRavenEtl) {
        super.update(dto);
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
    }

    editTask() {
        // TODO...
    }
}

export = ongoingTaskETLModel;
