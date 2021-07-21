/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class clientConfigurationModel {

    static readonly readModes = [
        { value: "None", label: "None"},
        { value: "RoundRobin", label: "Round Robin"},
        { value: "FastestNode", label: "Fastest node"}
    ] as Array<valueAndLabelItem<Raven.Client.Http.ReadBalanceBehavior, string>>;

    identityPartsSeparator = ko.observable<string>();
    separatorPlaceHolder: KnockoutComputed<string>;
        
    maxNumberOfRequestsPerSession = ko.observable<number>();
    requestsNumberPlaceHolder: KnockoutComputed<string>;
    
    useSessionContextForLoadBehavior = ko.observable<Raven.Client.Http.LoadBalanceBehavior>();
    loadBalanceContextSeed = ko.observable<number>();
    setLoadBalanceSeed = ko.observable<boolean>(false);
    
    readBalanceBehavior = ko.observable<Raven.Client.Http.ReadBalanceBehavior>("None");
    readBalanceBehaviorLabel: KnockoutComputed<string>;

    disabled = ko.observable<boolean>();
    isDefined = ko.observableArray<keyof this>([]);

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.Configuration.ClientConfiguration) {

        if (dto) {
            if (dto.IdentityPartsSeparator) {
                this.isDefined.push("identityPartsSeparator");
                this.identityPartsSeparator(dto.IdentityPartsSeparator);
            }
            
            if (dto.LoadBalanceBehavior === "UseSessionContext") {
                this.isDefined.push("useSessionContextForLoadBehavior");
                this.useSessionContextForLoadBehavior("UseSessionContext");
                
                if (dto.LoadBalancerContextSeed || dto.LoadBalancerContextSeed === 0) {
                    this.isDefined.push("loadBalanceContextSeed");
                    this.loadBalanceContextSeed(dto.LoadBalancerContextSeed);
                    this.setLoadBalanceSeed(true);
                }
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
        
        this.separatorPlaceHolder = ko.pureComputed(() => {
            return _.includes(this.isDefined(), "identityPartsSeparator") ? "Enter separator char" : "Default is '/'";
        });

        this.requestsNumberPlaceHolder = ko.pureComputed(() => {
            return _.includes(this.isDefined(), "maxNumberOfRequestsPerSession") ? "Enter requests number" : "Default is 30";
        });
        
        this.setLoadBalanceSeed.subscribe(setSeed => {
           if (setSeed) {
               this.isDefined.push("loadBalanceContextSeed");
           } else {
               _.remove(this.isDefined(), x => x === "loadBalanceContextSeed");
               this.loadBalanceContextSeed(null);
           }
        });
        
        this.isDefined.subscribe((changesList) => {
            const change = changesList[0];
            
            if (_.includes(this.isDefined(), "readBalanceBehavior")) {
                if (change.status === "added" && change.value === "useSessionContextForLoadBehavior") {
                    _.remove(this.isDefined(), x => x === "readBalanceBehavior");
                }
            }

            if (_.includes(this.isDefined(), "useSessionContextForLoadBehavior")) {
                if (change.status === "added" && change.value === "readBalanceBehavior") {
                    _.remove(this.isDefined(), x => x === "useSessionContextForLoadBehavior");
                    this.setLoadBalanceSeed(false);
                }
            }

            if (!_.includes(this.isDefined(), "useSessionContextForLoadBehavior")) {
                this.setLoadBalanceSeed(false);
            }
            
            if (!_.includes(this.isDefined(), "identityPartsSeparator")) {
                this.identityPartsSeparator(null);
            }

            if (!_.includes(this.isDefined(), "maxNumberOfRequestsPerSession")) {
                this.maxNumberOfRequestsPerSession(null);
            }

            if (!_.includes(this.isDefined(), "readBalanceBehavior")) {
                this.readBalanceBehavior("None");
            }
        }, null, "arrayChange");

        this.dirtyFlag = new ko.DirtyFlag([
            this.identityPartsSeparator,
            this.maxNumberOfRequestsPerSession,
            this.useSessionContextForLoadBehavior,
            this.setLoadBalanceSeed,
            this.loadBalanceContextSeed,
            this.readBalanceBehavior,
            this.isDefined
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.identityPartsSeparator.extend({
            required: {
                onlyIf: () => _.includes(this.isDefined(), "identityPartsSeparator")
            },
            validation: [
                {
                    validator: (val: string) => !_.includes(this.isDefined(), "identityPartsSeparator") || val.length === 1,
                    message: "Enter one character only"
                },
                {
                    validator: (val: string) => !_.includes(this.isDefined(), "identityPartsSeparator") || val !== "|",
                    message: "Identity parts separator cannot be set to '|'"
                }
            ]
        });

        this.readBalanceBehavior.extend({
            required: {
                onlyIf: () => _.includes(this.isDefined(), "readBalanceBehavior")
            }
        });
        
        this.maxNumberOfRequestsPerSession.extend({
            required: {
                onlyIf: () => _.includes(this.isDefined(), "maxNumberOfRequestsPerSession")
            },
            min: 0,
            digit: true
        });
        
        this.loadBalanceContextSeed.extend({
            required: {
                onlyIf: () => this.setLoadBalanceSeed()
            },
            min: 0,
            digit: true
        });
        
        this.validationGroup = ko.validatedObservable({
            identityPartsSeparator: this.identityPartsSeparator,
            readBalanceBehavior: this.readBalanceBehavior,
            maxNumberOfRequestsPerSession: this.maxNumberOfRequestsPerSession,
            loadBalanceContextSeed: this.loadBalanceContextSeed
        });
    }
    
    static empty() {
        return new clientConfigurationModel({
        } as Raven.Client.Documents.Operations.Configuration.ClientConfiguration);
    }
    
    toDto() {
        return {
            IdentityPartsSeparator: _.includes(this.isDefined(), "identityPartsSeparator") ? this.identityPartsSeparator() : null,
            LoadBalanceBehavior: _.includes(this.isDefined(), "useSessionContextForLoadBehavior") ? "UseSessionContext" : "None",
            LoadBalancerContextSeed: _.includes(this.isDefined(), "useSessionContextForLoadBehavior") && _.includes(this.isDefined(), "loadBalanceContextSeed") ? this.loadBalanceContextSeed() : null,
            ReadBalanceBehavior: _.includes(this.isDefined(), "readBalanceBehavior") ? this.readBalanceBehavior() : null,
            MaxNumberOfRequestsPerSession: _.includes(this.isDefined(), "maxNumberOfRequestsPerSession") ? this.maxNumberOfRequestsPerSession() : null,
            Disabled: this.disabled()
        } as Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
    }
}

export = clientConfigurationModel;
