import viewModelBase = require("viewmodels/viewModelBase");
import clientConfigurationModel = require("models/database/settings/clientConfigurationModel");
import getGlobalClientConfigurationCommand = require("commands/resources/getGlobalClientConfigurationCommand");
import saveClientConfigurationCommand = require("commands/resources/saveClientConfigurationCommand");
import getClientConfigurationCommand = require("commands/resources/getClientConfigurationCommand");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import accessManager = require("common/shell/accessManager");

class clientConfiguration extends viewModelBase {
    model: clientConfigurationModel;
    globalModel: clientConfigurationModel;
    hasGlobalConfiguration = ko.observable<boolean>(false);
    serverWideClientConfigurationUrl = appUrl.forGlobalClientConfiguration();
    canNavigateToServerSettings: KnockoutComputed<boolean>;
    
    overrideServer = ko.observable<boolean>(false);
    canEditSettings: KnockoutComputed<boolean>;
    
    effectiveIdentityPartsSeparator: KnockoutComputed<string>;
    effectiveReadBalanceBehavior: KnockoutComputed<string>;
    effectiveMaxNumberOfRequestsPerSession: KnockoutComputed<string>;
     
    separatorPlaceHolderInSettings: KnockoutComputed<string>;
    requestsNumberPlaceHolderInSettings: KnockoutComputed<string>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    isSaveEnabled: KnockoutComputed<boolean>;

    activate(args: any) {
        super.activate(args);

        this.bindToCurrentInstance("saveConfiguration", "setReadMode");
        
        this.canNavigateToServerSettings = accessManager.default.isClusterAdminOrClusterNode;
        
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

        this.overrideServer.subscribe(override => {
            if (override) {
                this.model.isDefined([...this.globalModel.isDefined()]);
            } else {
                this.model.isDefined.removeAll();
            }
        });
    }

    private initObservables() {
        this.effectiveIdentityPartsSeparator = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }

            if (this.overrideServer()) {
                const configToUse = this.model;
                const separator = this.model.identityPartsSeparator();
                return _.includes(configToUse.isDefined(), "identityPartsSeparator") && separator ? separator :  "'/' (default)";
            } else {
                const separator = this.globalModel.identityPartsSeparator();
                return separator || "'/' (default)"; 
            }
        });

        this.effectiveReadBalanceBehavior = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }

            const configToUse = this.overrideServer() ? this.model : this.globalModel;
            const usingSessionContext = _.includes(configToUse.isDefined(), "useSessionContextForLoadBehavior");
            
            if (usingSessionContext) {
                const isSeedDefined =  _.includes(configToUse.isDefined(), "loadBalanceContextSeed") && 
                                       (configToUse.loadBalanceContextSeed() || configToUse.loadBalanceContextSeed() === 0);
                
                const seedText = isSeedDefined ? `(Seed: ${configToUse.loadBalanceContextSeed()})` : "";
                return `Session Context ${seedText}`;
            }

            const label = configToUse.readBalanceBehaviorLabel();
            if (this.overrideServer()) {
                return _.includes(configToUse.isDefined(), "readBalanceBehavior") ? label : "None (default)";
            } else {
                return label || "None (default)";
            }
        });

        this.effectiveMaxNumberOfRequestsPerSession = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration()) {
                return "";
            }

            if (this.overrideServer()) {
                const configToUse = this.model;
                const maxRequests = this.model.maxNumberOfRequestsPerSession();
                return _.includes(configToUse.isDefined(), "maxNumberOfRequestsPerSession") && maxRequests ? maxRequests.toLocaleString() : "30 (default)";
            } else {
                const maxRequests = this.globalModel.maxNumberOfRequestsPerSession();
                return maxRequests ? maxRequests.toLocaleString() : "30 (default)";
            }
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
        
        this.separatorPlaceHolderInSettings = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration() || this.overrideServer()) {
                return this.model.separatorPlaceHolder();
            }
            
            return "";
        })

        this.requestsNumberPlaceHolderInSettings = ko.pureComputed(() => {
            if (!this.hasGlobalConfiguration() || this.overrideServer()) {
                return this.model.requestsNumberPlaceHolder();
            }

            return "";
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
