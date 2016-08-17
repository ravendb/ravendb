class clusterConfiguration {

    enableReplication = ko.observable<boolean>();
    disableReplicationStateChecks = ko.observable<boolean>();
    databaseSettings = ko.observable<dictionary<string>>();
    constructor(dto: clusterConfigurationDto) {
        this.enableReplication(dto.EnableReplication);
        this.disableReplicationStateChecks(dto.DisableReplicationStateChecks);
        this.databaseSettings(dto.DatabaseSettings);
    }

    toDto(): clusterConfigurationDto {
        return {
            EnableReplication: this.enableReplication(),
            DisableReplicationStateChecks: this.disableReplicationStateChecks() ,
            DatabaseSettings: this.databaseSettings()
    };
    }

    static empty() {
        return new clusterConfiguration({
            EnableReplication: false,
            DisableReplicationStateChecks: false,
            DatabaseSettings:null
    });
    }
}

export = clusterConfiguration;
