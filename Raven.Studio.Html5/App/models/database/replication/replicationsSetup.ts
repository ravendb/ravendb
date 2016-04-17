import replicationDestination = require("models/database/replication/replicationDestination");

class replicationsSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>().extend({ required: true });
    clientFailoverBehaviour = ko.observable<string>(null);
    showCustomRequestTimeThreshold: KnockoutObservable<boolean>;
    hasCustomRequestTimeThreshold = ko.observable<boolean>(false);
    requestTimeThreshold = ko.observable<number>(null);

    constructor(dto: configurationDocumentDto<replicationsDto>) {
        this.source(dto.MergedDocument.Source);
        this.destinations(dto.MergedDocument.Destinations.map(dest => {
            var result = new replicationDestination(dest);
            if (dto.GlobalDocument) {
                var foundParent = dto.GlobalDocument.Destinations
                    .first(x => x.Url.toLowerCase() === dest.Url.toLowerCase() && x.Database.toLowerCase() === dest.Database.toLowerCase());
                if (foundParent) {
                    result.globalConfiguration(new replicationDestination(foundParent));
                }
            }
            return result;
        }));
        var clientConfiguration = dto.MergedDocument.ClientConfiguration;
        if (clientConfiguration) {
            if (clientConfiguration.FailoverBehavior) {
                this.clientFailoverBehaviour(clientConfiguration.FailoverBehavior);
            }
            if (clientConfiguration.RequestTimeThresholdInMilliseconds) {
                this.hasCustomRequestTimeThreshold(true);
                this.requestTimeThreshold(clientConfiguration.RequestTimeThresholdInMilliseconds);
            }
        }
        this.showCustomRequestTimeThreshold = ko.computed(() => {
            return this.clientFailoverBehaviour() === "AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed";
        });

        this.clientFailoverBehaviour.subscribe(newValue => {
            if (newValue !== 'AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed') {
                this.hasCustomRequestTimeThreshold(false);
                this.requestTimeThreshold(undefined);
            }
        });

    }

    toDto(filterLocal = true): replicationsDto {
        var dto: replicationsDto = {
            Destinations: this.destinations().filter(dest => !filterLocal || dest.hasLocal()).map(dest => dest.toDto()),
            Source: this.source()
        };
        dto.ClientConfiguration = {
            RequestTimeThresholdInMilliseconds: undefined,
            FailoverBehavior: undefined
        };

        if (this.clientFailoverBehaviour()) {
            dto.ClientConfiguration.FailoverBehavior = this.clientFailoverBehaviour();
        }
        if (this.showCustomRequestTimeThreshold() && this.hasCustomRequestTimeThreshold()) {
            dto.ClientConfiguration.RequestTimeThresholdInMilliseconds = this.requestTimeThreshold();
        }

        return dto;
    }

    copyFromParent(parentClientFailover: string, parentRequestTimeThreshold: number) {
        this.clientFailoverBehaviour(parentClientFailover);
        this.requestTimeThreshold(parentRequestTimeThreshold);
        this.destinations(this.destinations().filter(d => d.hasGlobal()));
        this.destinations().forEach(d => d.copyFromGlobal());
        this.hasCustomRequestTimeThreshold(!!parentRequestTimeThreshold);
    }

    clear() {
        this.destinations.removeAll();
        this.clientFailoverBehaviour(null);
        this.source(null);
    }
}

export = replicationsSetup;
