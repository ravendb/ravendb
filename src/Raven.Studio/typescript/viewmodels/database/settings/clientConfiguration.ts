import viewModelBase = require("viewmodels/viewModelBase");
import clientConfigurationModel = require("models/database/settings/clientConfigurationModel");
import getGlobalClientConfigurationCommand = require("commands/resources/getGlobalClientConfigurationCommand");
import saveClientConfigurationCommand = require("commands/resources/saveClientConfigurationCommand");
import getClientConfigurationCommand = require("commands/resources/getClientConfigurationCommand");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");

class clientConfiguration extends viewModelBase {
    model: clientConfigurationModel;
    globalModel: clientConfigurationModel;
    hasGlobalConfiguration = ko.observable<boolean>(false);
    overrideServer = ko.observable<boolean>(false);
    
    globalConfigUrl = appUrl.forGlobalClientConfiguration();
    
    effectiveReadBalanceBehavior: KnockoutComputed<string>;
    effectiveMaxNumberOfRequestsPerSession: KnockoutComputed<string>;
    canEditSettings: KnockoutComputed<boolean>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    constructor() {
        super();
        
        this.effectiveReadBalanceBehavior = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }
            
            const configToUse = this.overrideServer() ? this.model : this.globalModel;
            const label = configToUse.readBalanceBehaviorLabel();
            return _.includes(configToUse.isDefined(), "readBalanceBehavior") ? label : "<use client default>";
        });
        
        this.effectiveMaxNumberOfRequestsPerSession = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }
            
            const configToUse = this.overrideServer() ? this.model : this.globalModel;
            const maxRequests = configToUse.maxNumberOfRequestsPerSession();
            
            return _.includes(configToUse.isDefined(), "maxNumberOfRequestsPerSession") && maxRequests ? maxRequests.toLocaleString() :  '<use client default>';            
        });
        
        this.canEditSettings = ko.pureComputed(() => {
            const hasGlobal = this.hasGlobalConfiguration();
            const override = this.overrideServer();
            return !hasGlobal || override;
        })
    }

    activate(args: any) {
        super.activate(args);

        this.bindToCurrentInstance("saveConfiguration", "setReadMode");

        const globalTask = new getGlobalClientConfigurationCommand()
            .execute()
            .done(dto => {
                this.globalModel = new clientConfigurationModel(dto);
                this.hasGlobalConfiguration(dto && !dto.Disabled);
            });
        
        const localTask = new getClientConfigurationCommand(this.activeDatabase())
            .execute()  
            .done((dto) => {
                this.model = new clientConfigurationModel(dto);
            });
        
        return $.when<any>(localTask, globalTask)
            .done(() => {
                this.overrideServer(this.hasGlobalConfiguration() && !this.model.disabled());
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.initValidation();

        this.overrideServer.subscribe(override => {
            if (override) {
                this.model.isDefined(this.globalModel.isDefined());
            } else {
                this.model.isDefined.removeAll();
            }
        });
    }

    private initValidation() {
        this.model.readBalanceBehavior.extend({
            required: {
                onlyIf: () => _.includes(this.model.isDefined(), "readBalanceBehavior")
            }
        });

        this.model.maxNumberOfRequestsPerSession.extend({
            required: {
                onlyIf: () => _.includes(this.model.isDefined(), "maxNumberOfRequestsPerSession")
            },
            digit: true
        })
    }

    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        eventsCollector.default.reportEvent("client-configuration", "save");

        this.spinners.save(true);
        this.model.disabled(this.hasGlobalConfiguration() ? !this.overrideServer() : this.model.isDefined().length === 0);

        new saveClientConfigurationCommand(this.model.toDto(), this.activeDatabase())
            .execute()
            .always(() => this.spinners.save(false));
    }

    setReadMode(mode: Raven.Client.Http.ReadBalanceBehavior) {
        this.model.readBalanceBehavior(mode);
    }
}

export = clientConfiguration;
