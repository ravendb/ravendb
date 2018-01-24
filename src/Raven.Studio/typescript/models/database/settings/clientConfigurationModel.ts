/// <reference path="../../../../typings/tsd.d.ts"/>

class clientConfigurationModel {

    static readonly readModes = [
        { value: "None", label: "None"},
        { value: "RoundRobin", label: "Round Robin"},
        { value: "FastestNode", label: "Fastest node"}
    ] as Array<valueAndLabelItem<Raven.Client.Http.ReadBalanceBehavior, string>>;

    readBalanceBehavior = ko.observable<Raven.Client.Http.ReadBalanceBehavior>('None');
    maxNumberOfRequestsPerSession = ko.observable<number>();
    disabled = ko.observable<boolean>();

    readBalanceBehaviorLabel = ko.pureComputed(() => {
        const value = this.readBalanceBehavior();
        const kv = clientConfigurationModel.readModes;
        return value ? kv.find(x => x.value === value).label : "None";
    });
    
    isDefined = ko.observableArray<keyof this>([]);
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.Configuration.ClientConfiguration) {
        if (dto && dto.ReadBalanceBehavior != null) {
            this.isDefined.push("readBalanceBehavior");
            this.readBalanceBehavior(dto.ReadBalanceBehavior);
        }
        
        if (dto && dto.MaxNumberOfRequestsPerSession != null) {
            this.isDefined.push("maxNumberOfRequestsPerSession");
            this.maxNumberOfRequestsPerSession(dto.MaxNumberOfRequestsPerSession);
        }
        
        this.disabled(!dto || dto.Disabled);        
        this.initValidation();
    }
    
    private initValidation() {
        this.maxNumberOfRequestsPerSession.extend({
            min: 0
        });
        
        this.validationGroup = ko.validatedObservable({
            readBalanceBehavior: this.readBalanceBehavior,
            maxNumberOfRequestsPerSession: this.maxNumberOfRequestsPerSession
        });
    }
    
    static empty() {
        return new clientConfigurationModel({
        } as Raven.Client.Documents.Operations.Configuration.ClientConfiguration);
    }
    
    toDto() {
        return {
            ReadBalanceBehavior: _.includes(this.isDefined(), "readBalanceBehavior") ? this.readBalanceBehavior() : null,
            MaxNumberOfRequestsPerSession: _.includes(this.isDefined(), "maxNumberOfRequestsPerSession") ? this.maxNumberOfRequestsPerSession() : null,
            Disabled: this.disabled()
        } as Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
    }
}

export = clientConfigurationModel;
