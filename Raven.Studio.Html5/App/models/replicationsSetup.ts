import replicationDestination = require("models/replicationDestination");

class replicationsSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>().extend({ required: true });
    clientFailoverBehaviour = ko.observable<string>(null);

    constructor(dto: replicationsDto) {
        this.source(dto.Source);
        this.destinations(dto.Destinations.map(dest => new replicationDestination(dest)));
        if (dto.ClientConfiguration && dto.ClientConfiguration.FailoverBehavior) {
            this.clientFailoverBehaviour(dto.ClientConfiguration.FailoverBehavior);
        }
    }

    toDto(): replicationsDto {
        var dto: replicationsDto = {
            Destinations: this.destinations().map(dest => dest.toDto()),
            Source: this.source()
        };

        if (this.clientFailoverBehaviour()) {
            dto.ClientConfiguration = { FailoverBehavior: this.clientFailoverBehaviour() };
        }

        return dto;
    }
}

export = replicationsSetup;