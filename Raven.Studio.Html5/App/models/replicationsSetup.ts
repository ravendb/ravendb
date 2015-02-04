import replicationDestination = require("models/replicationDestination");

class replicationsSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>().extend({ required: true });
    clientFailoverBehaviour = ko.observable<string>(null);

    constructor(dto: configurationDocumentDto<replicationsDto>) {
        this.source(dto.MergedDocument.Source);
        this.destinations(dto.MergedDocument.Destinations.map(dest => {
            var result = new replicationDestination(dest);
            if (dto.GlobalDocument) {
                var foundParent = dto.GlobalDocument.Destinations
                    .first(x => x.Url.toLowerCase() == dest.Url.toLowerCase() && x.Database.toLowerCase() == dest.Database.toLowerCase());
                if (foundParent) {
                    result.globalConfiguration(new replicationDestination(foundParent));
                }
            }
            return result;
        }));
        if (dto.MergedDocument.ClientConfiguration && dto.MergedDocument.ClientConfiguration.FailoverBehavior) {
            this.clientFailoverBehaviour(dto.MergedDocument.ClientConfiguration.FailoverBehavior);
        }
    }

    toDto(): replicationsDto {
        var dto: replicationsDto = {
            Destinations: this.destinations().filter(dest => dest.hasLocal()).map(dest => dest.toDto()),
            Source: this.source()
        };

        if (this.clientFailoverBehaviour()) {
            dto.ClientConfiguration = { FailoverBehavior: this.clientFailoverBehaviour() };
        }

        return dto;
    }
}

export = replicationsSetup;