import replicationDestination = require("models/database/replication/replicationDestination");

class replicationsSetup {
    /* TODO
    source = ko.observable<string>();
    destinations = ko.observableArray<replicationDestination>();
    clientFailoverBehaviour = ko.observable<string>(null);
    showRequestTimeSlaThreshold: KnockoutObservable<boolean>;
    requestTimeSlaThreshold = ko.observable<number>(null);
    hasAnyReplicationDestination: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Documents.Replication.ReplicationDocument<Raven.Client.Documents.Replication.ReplicationDestination>) {
        this.source(dto.Source);

        this.destinations(dto.Destinations.map(d => new replicationDestination(d)));

        const clientConfiguration = dto.ClientConfiguration;
        //if (clientConfiguration) {
        //    if (clientConfiguration.FailoverBehavior) {
        //        this.clientFailoverBehaviour(clientConfiguration.FailoverBehavior);
        //    }
        //}
        this.showRequestTimeSlaThreshold = ko.computed(() => {
            return this.clientFailoverBehaviour() && this.clientFailoverBehaviour().includes("AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
        });

        this.clientFailoverBehaviour.subscribe(newValue => {
            if (!newValue.includes('AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached')) {
                this.requestTimeSlaThreshold(undefined);
            } else if (!this.requestTimeSlaThreshold()) {
                this.requestTimeSlaThreshold(100);
            }
        });
        this.hasAnyReplicationDestination = ko.computed(() => this.destinations().filter(x => !x.enableReplicateOnlyFromCollections()).length > 0);
    }

    toDto(filterLocal = true): replicationsDto {
        var dto: replicationsDto = {
            Destinations: this.destinations().map(dest => dest.toDto()),
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
        return _.includes(tokens, "ReadFromAllServers") && _.includes(tokens, "AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    readFromAllButSwitchWhenRequestTimeSlaThresholdIsReached = ko.computed(() => {
        var behaviour = this.clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",").map(x => x.trim());
        return _.includes(tokens, "ReadFromAllServers") && _.includes(tokens, "AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
    });*/
}

export = replicationsSetup;
