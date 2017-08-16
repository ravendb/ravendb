/// <reference path="../../../../typings/tsd.d.ts"/>

import clientConfigurationModel = require("models/database/settings/clientConfigurationModel");

class globalClientConfigurationModel {

    static readonly readModes = clientConfigurationModel.readModes;

    readBalanceBehavior = ko.observable<Raven.Client.Http.ReadBalanceBehavior>('None');
    maxNumberOfRequestsPerSession = ko.observable<number>();
    
    isDefined = ko.observableArray<keyof this>([]);
    
    validationGroup = ko.validatedObservable({
        readBalanceBehavior: this.readBalanceBehavior,
        maxNumberOfRequestsPerSession: this.maxNumberOfRequestsPerSession
    });
    
    constructor(dto: Raven.Client.ServerWide.ClientConfiguration) {
        if (dto.ReadBalanceBehavior != null) {
            this.isDefined.push("readBalanceBehavior");
            this.readBalanceBehavior(dto.ReadBalanceBehavior);
        }
        
        if (dto.MaxNumberOfRequestsPerSession != null) {
            this.isDefined.push("maxNumberOfRequestsPerSession");
            this.maxNumberOfRequestsPerSession(dto.MaxNumberOfRequestsPerSession);
        }
    }
    
    static empty() {
        return new globalClientConfigurationModel({
        } as Raven.Client.ServerWide.ClientConfiguration);
    }
    
    toDto() {
        return {
            ReadBalanceBehavior: _.includes(this.isDefined(), "readBalanceBehavior") ? this.readBalanceBehavior() : null,
            MaxNumberOfRequestsPerSession: _.includes(this.isDefined(), "maxNumberOfRequestsPerSession") ? this.maxNumberOfRequestsPerSession() : null,
            Disabled: this.isDefined().length === 0
        } as Raven.Client.ServerWide.ClientConfiguration;
    }
    
}

export = globalClientConfigurationModel;
