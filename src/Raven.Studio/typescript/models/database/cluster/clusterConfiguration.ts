/// <reference path="../../../../typings/tsd.d.ts"/>

class clusterConfiguration {

    enableReplication = ko.observable<boolean>();

    constructor(dto: clusterConfigurationDto) {
        this.enableReplication(dto.EnableReplication);
    }

    toDto(): clusterConfigurationDto {
        return {
            EnableReplication: this.enableReplication()
        };
    }

    static empty() {
        return new clusterConfiguration({
            EnableReplication: false
        });
    }
}

export = clusterConfiguration;
