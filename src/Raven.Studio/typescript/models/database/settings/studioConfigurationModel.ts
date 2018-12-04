/// <reference path="../../../../typings/tsd.d.ts"/>

interface globalStudioConfigurationOptions extends Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration {
    SendUsageStats: boolean;
}

class studioConfigurationModel {

    static readonly environments = ["None", "Development", "Testing", "Production"] as Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>; 
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    sendUsageStats = ko.observable<boolean>(false);
    disabled = ko.observable<boolean>();
    replicationFactor = ko.observable<number>(null);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: globalStudioConfigurationOptions) {
        this.initValidation();
        
        this.environment(dto.Environment);
        this.disabled(dto.Disabled);
        this.sendUsageStats(dto.SendUsageStats);
        this.replicationFactor(dto.ReplicationFactor);
    }
    
    private initValidation() {
        this.validationGroup = ko.validatedObservable({
            environment: this.environment
        });
    }
    
    toRemoteDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration {
        return {
            Environment: this.environment(),
            Disabled: this.disabled(),
            ReplicationFactor: this.replicationFactor()
        }
    }
}

export = studioConfigurationModel;
