import replicationDestination = require("models/replicationDestination");

class replicationsSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>().extend({ required: true });

    constructor(dto: replicationsDto) {
        this.source(dto.Source);
        this.destinations(dto.Destinations.map(dest => new replicationDestination(dest)));
    }

    toDto(): replicationsDto {
        return {
            Destinations: this.destinations().map(dest => dest.toDto()),
            Source: this.source()
        };
    }
}

export = replicationsSetup;