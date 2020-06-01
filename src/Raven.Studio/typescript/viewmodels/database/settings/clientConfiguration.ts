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
    globalConfigUrl = appUrl.forGlobalClientConfiguration();
    
    overrideServer = ko.observable<boolean>(false);
    canEditSettings: KnockoutComputed<boolean>;
    
    effectiveReadBalanceBehavior: KnockoutComputed<string>;
    effectiveMaxNumberOfRequestsPerSession: KnockoutComputed<string>;
     
    spinners = {
        save: ko.observable<boolean>(false)
    };

    isSaveEnabled: KnockoutComputed<boolean>;

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
            .done((dto) => this.model = new clientConfigurationModel(dto));
        
        return $.when<any>(localTask, globalTask)
            .done(() => this.initObservables());
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

    private initObservables() {
        this.effectiveReadBalanceBehavior = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }

            const configToUse = this.overrideServer() ? this.model : this.globalModel;
            const label = configToUse.readBalanceBehaviorLabel();

            const usingSessionContext = _.includes(configToUse.isDefined(), "useSessionContextForLoadBehavior");
            
            return usingSessionContext ? "<Session Context>" :
                _.includes(configToUse.isDefined(), "readBalanceBehavior") ? label : "<Client Default>";
        });

        this.effectiveMaxNumberOfRequestsPerSession = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }

            const configToUse = this.overrideServer() ? this.model : this.globalModel;
            const maxRequests = configToUse.maxNumberOfRequestsPerSession();

            return _.includes(configToUse.isDefined(), "maxNumberOfRequestsPerSession") && maxRequests ? maxRequests.toLocaleString() :  "<Client Default>";
        });

        this.canEditSettings = ko.pureComputed(() => {
            const hasGlobal = this.hasGlobalConfiguration();
            const override = this.overrideServer();
            return !hasGlobal || override;
        })

        this.dirtyFlag = new ko.DirtyFlag([
            this.model,
            this.overrideServer
        ], false);

        this.isSaveEnabled = ko.pureComputed(() => {
            const isDirty = this.dirtyFlag().isDirty();
            return isDirty && !this.spinners.save();
        });

        this.overrideServer(this.hasGlobalConfiguration() && !this.model.disabled());
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
            .done(() => this.dirtyFlag().reset())
            .always(() => this.spinners.save(false));
    }

    setReadMode(mode: Raven.Client.Http.ReadBalanceBehavior) {
        this.model.readBalanceBehavior(mode);
    }
}

export = clientConfiguration;
