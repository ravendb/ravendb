import replicationDestination = require("models/database/replication/replicationDestination");

class replicationsSetup {

    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>().extend({ required: true });
    clientFailoverBehaviour = ko.observable<string>(null);
    showRequestTimeSlaThreshold: KnockoutObservable<boolean>;
    requestTimeSlaThreshold = ko.observable<number>(null);
    hasAnyReplicationDestination: KnockoutComputed<boolean>;

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
            if (clientConfiguration.RequestTimeSlaThresholdInMilliseconds) {
                this.requestTimeSlaThreshold(clientConfiguration.RequestTimeSlaThresholdInMilliseconds);
            }
        }
        this.showRequestTimeSlaThreshold = ko.computed(() => {
            return this.clientFailoverBehaviour() && this.clientFailoverBehaviour().contains("AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
        });

        this.clientFailoverBehaviour.subscribe(newValue => {
            if (!newValue.contains('AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached')) {
                this.requestTimeSlaThreshold(undefined);
            } else if (!this.requestTimeSlaThreshold()) {
                this.requestTimeSlaThreshold(100);
            }
        });
        this.hasAnyReplicationDestination = ko.computed(() => this.destinations().filter(x => !x.enableReplicateOnlyFromCollections()).length > 0);
    }

    toDto(filterLocal = true): replicationsDto {
        var dto: replicationsDto = {
            Destinations: this.destinations().filter(dest => !filterLocal || dest.hasLocal()).map(dest => dest.toDto()),
            Source: this.source()
        };
        dto.ClientConfiguration = {
            RequestTimeSlaThresholdInMilliseconds: undefined,
            FailoverBehavior: undefined
        };

        if (this.clientFailoverBehaviour()) {
            dto.ClientConfiguration.FailoverBehavior = this.clientFailoverBehaviour();
        }
        if (this.showRequestTimeSlaThreshold()) {
            dto.ClientConfiguration.RequestTimeSlaThresholdInMilliseconds = this.requestTimeSlaThreshold();
        }

        return dto;
    }

    copyFromParent(parentClientFailover: string, parentRequestTimeSlaThreshold: number) {
        this.clientFailoverBehaviour(parentClientFailover);
        this.requestTimeSlaThreshold(parentRequestTimeSlaThreshold);
        this.destinations(this.destinations().filter(d => d.hasGlobal()));
        this.destinations().forEach(d => d.copyFromGlobal());
    }

    clear() {
        this.destinations.removeAll();
        this.clientFailoverBehaviour(null);
        this.source(null);
    }

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",").map(x => x.trim());
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    readFromAllButSwitchWhenRequestTimeSlaThresholdIsReached = ko.computed(() => {
        var behaviour = this.clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",").map(x => x.trim());
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
    });
}

export = replicationsSetup;
