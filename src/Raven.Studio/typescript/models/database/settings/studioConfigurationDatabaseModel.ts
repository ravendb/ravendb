/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class studioConfigurationDatabaseModel {

    static readonly environments: Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment> = ["None", "Development", "Testing", "Production"];
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    disabled = ko.observable<boolean>();
    disableAutoIndexCreation = ko.observable<boolean>();

    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) {
        this.environment(dto ? dto.Environment : "None");
        this.disabled(dto ? dto.Disabled : false);
        this.disableAutoIndexCreation(dto.DisableAutoIndexCreation);

        this.dirtyFlag = new ko.DirtyFlag([
            this.environment,
            this.disableAutoIndexCreation
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    static empty() {
        return new studioConfigurationDatabaseModel({
            Disabled: false,
            Environment: "None",
            DisableAutoIndexCreation: false
        });
    }
    
    toRemoteDto(): Raven.Client.Documents.Operations.Configuration.StudioConfiguration {
        return {
            Environment: this.environment(),
            Disabled: this.disabled(),
            DisableAutoIndexCreation: this.disableAutoIndexCreation()
        }
    }
}

export = studioConfigurationDatabaseModel;
