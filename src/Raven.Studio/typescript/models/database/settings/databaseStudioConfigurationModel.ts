/// <reference path="../../../../typings/tsd.d.ts"/>
class databaseStudioConfigurationModel {

    static readonly environments = ["None", "Development", "Testing", "Production"] as Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>;
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    disabled = ko.observable<boolean>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) {
        this.initValidation();
        
        this.environment(dto ? dto.Environment : "None");
        this.disabled(dto ? dto.Disabled : false);
    }
    
    private initValidation() {
        this.validationGroup = ko.validatedObservable({
            environment: this.environment
        });
    }
    
    static empty() {
        return new databaseStudioConfigurationModel({
            Disabled: false,
            Environment: "None"
        });
    }
    
    toRemoteDto(): Raven.Client.Documents.Operations.Configuration.StudioConfiguration {
        return {
            Environment: this.environment(),
            Disabled: this.disabled()
        }
    }
}

export = databaseStudioConfigurationModel;
