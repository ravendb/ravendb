/// <reference path="../../../../typings/tsd.d.ts"/>

import jsonUtil = require("common/jsonUtil");

interface globalStudioConfigurationOptions extends Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration {
    SendUsageStats: boolean;
    CollapseDocsWhenOpening: boolean;
}

class studioConfigurationModel {

    static readonly environments = ["None", "Development", "Testing", "Production"] as Array<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>; 
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    sendUsageStats = ko.observable<boolean>(false);
    disabled = ko.observable<boolean>();
    replicationFactor = ko.observable<number>(null);
    collapseDocsWhenOpening = ko.observable<boolean>();

    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: globalStudioConfigurationOptions) {
        this.initValidation();
        
        this.environment(dto.Environment);
        this.disabled(dto.Disabled);
        this.sendUsageStats(dto.SendUsageStats);
        this.replicationFactor(dto.ReplicationFactor);
        this.collapseDocsWhenOpening(dto.CollapseDocsWhenOpening);

        this.dirtyFlag = new ko.DirtyFlag([
            this.environment,
            this.sendUsageStats,
            this.replicationFactor,
            this.collapseDocsWhenOpening
        ], false, jsonUtil.newLineNormalizingHashFunction);
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
