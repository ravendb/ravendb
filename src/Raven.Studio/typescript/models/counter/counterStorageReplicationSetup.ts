import counterStorageReplicationDestination = require("models/counter/counterStorageReplicationDestination");

class counterStorageReplicationSetup {

    destinations = ko.observableArray<counterStorageReplicationDestination>().extend({ required: true });

    constructor(dto: counterStorageReplicationDto) {
        this.destinations(dto.Destinations.map(dest => new counterStorageReplicationDestination(dest)));
    }

    toDto(): counterStorageReplicationDto {
        return {
            Destinations: this.destinations().map(dest => dest.toDto())
        };
    }
}

export = counterStorageReplicationSetup;
