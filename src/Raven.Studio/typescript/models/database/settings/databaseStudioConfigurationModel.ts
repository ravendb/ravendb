/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class databaseStudioConfigurationModel {

    static readonly environments = ["None", "Development", "Testing", "Production"] as Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>;
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    disabled = ko.observable<boolean>();

    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) {
        this.initValidation();
        
        this.environment(dto ? dto.Environment : "None");
        this.disabled(dto ? dto.Disabled : false);

        this.dirtyFlag = new ko.DirtyFlag([
            this.environment
        ], false, jsonUtil.newLineNormalizingHashFunction);
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
