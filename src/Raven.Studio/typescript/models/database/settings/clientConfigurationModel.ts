/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class clientConfigurationModel {

    static readonly readModes = [
        { value: "None", label: "None"},
        { value: "RoundRobin", label: "Round Robin"},
        { value: "FastestNode", label: "Fastest node"}
    ] as Array<valueAndLabelItem<Raven.Client.Http.ReadBalanceBehavior, string>>;

    maxNumberOfRequestsPerSession = ko.observable<number>();
    useSessionContextForLoadBehavior = ko.observable<Raven.Client.Http.LoadBalanceBehavior>();
    
    readBalanceBehavior = ko.observable<Raven.Client.Http.ReadBalanceBehavior>('None');
    readBalanceBehaviorLabel: KnockoutComputed<string>;

    disabled = ko.observable<boolean>();
    isDefined = ko.observableArray<keyof this>([]);

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.Configuration.ClientConfiguration) {

        if (dto) {
            if (dto.LoadBalanceBehavior === "UseSessionContext") {
                this.isDefined.push("useSessionContextForLoadBehavior");
                this.useSessionContextForLoadBehavior("UseSessionContext");
            } else if (dto.ReadBalanceBehavior != null) {
                this.isDefined.push("readBalanceBehavior");
                this.readBalanceBehavior(dto.ReadBalanceBehavior);
            }

            if (dto.MaxNumberOfRequestsPerSession != null) {
                this.isDefined.push("maxNumberOfRequestsPerSession");
                this.maxNumberOfRequestsPerSession(dto.MaxNumberOfRequestsPerSession);
            }
        }

        this.disabled(!dto || dto.Disabled);

        this.initObservables();
        this.initValidation();
    }

    initObservables() {
        this.readBalanceBehaviorLabel = ko.pureComputed(() => {
            const value = this.readBalanceBehavior();
            const kv = clientConfigurationModel.readModes;
            return value ? kv.find(x => x.value === value).label : "None";
        });

        this.isDefined.subscribe(() => {
            if (this.isDefined().find(x => x === "useSessionContextForLoadBehavior")) {
                this.isDefined.remove("readBalanceBehavior");
            }
        })

        this.dirtyFlag = new ko.DirtyFlag([
            this.maxNumberOfRequestsPerSession,
            this.useSessionContextForLoadBehavior,
            this.readBalanceBehavior,
            this.isDefined
        ], false, jsonUtil.newLineNormalizingHashFunction);
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
            LoadBalanceBehavior: _.includes(this.isDefined(), "useSessionContextForLoadBehavior") ? "UseSessionContext" : null,
            ReadBalanceBehavior: _.includes(this.isDefined(), "readBalanceBehavior") ? this.readBalanceBehavior() : null,
            MaxNumberOfRequestsPerSession: _.includes(this.isDefined(), "maxNumberOfRequestsPerSession") ? this.maxNumberOfRequestsPerSession() : null,
            Disabled: this.disabled()
        } as Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
    }
}

export = clientConfigurationModel;
