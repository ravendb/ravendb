/// <reference path="../../../../typings/tsd.d.ts"/>

interface globalStudionConfigurationOptions extends Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration {
    SendUsageStats: boolean;
}

class studioConfigurationModel {

    static readonly environments = ["None", "Development", "Testing", "Production"] as Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>; 
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    sendUsageStats = ko.observable<boolean>(false);
    disabled = ko.observable<boolean>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: globalStudionConfigurationOptions) {
        this.initValidation();
        
        this.environment(dto.Environment);
        this.disabled(dto.Disabled);
        this.sendUsageStats(dto.SendUsageStats);
    }
    
    private initValidation() {
        this.validationGroup = ko.validatedObservable({
            environment: this.environment
        });
    }
    
    toRemoteDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration {
        return {
            Environment: this.environment(),
            Disabled: this.disabled()
        }
    }
}

export = studioConfigurationModel;
