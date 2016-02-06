import synchronizationDestination = require("models/filesystem/synchronizationDestination");

class synchronizationReplicationSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<synchronizationDestination>().extend({ required: true });

    constructor(dto: synchronizationReplicationsDto) {
        this.source(dto.Source);
        this.destinations(dto.Destinations.map(dest => new synchronizationDestination(dest)));
    }

    toDto(): synchronizationReplicationsDto {
        return {
            Destinations: this.destinations().map(dest => dest.toDto()),
            Source: this.source()
        };
    }
}

export = synchronizationReplicationSetup;
