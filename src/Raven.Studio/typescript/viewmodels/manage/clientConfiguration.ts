import viewModelBase = require("viewmodels/viewModelBase");
import globalClientConfigurationModel = require("models/database/settings/globalClientConfigurationModel");
import saveGlobalClientConfigurationCommand = require("commands/resources/saveGlobalClientConfigurationCommand");
import getGlobalClientConfigurationCommand = require("commands/resources/getGlobalClientConfigurationCommand");

class clientConfiguration extends viewModelBase {

    model: globalClientConfigurationModel;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    readBalanceBehaviorLabel = ko.pureComputed(() => {
        const value = this.model.readBalanceBehavior();
        const kv = globalClientConfigurationModel.readModes;
        return value ? kv.find(x => x.value === value).label : "None";
    });
    
    activate(args: any) {
        super.activate(args);
        
        this.bindToCurrentInstance("saveConfiguration", "setReadMode");
        
        return new getGlobalClientConfigurationCommand()
            .execute()
            .done((dto) => {
                this.model = new globalClientConfigurationModel(dto);
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.initValidation();
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
            }
        })
    }
    
    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        this.spinners.save(true);
        
        new saveGlobalClientConfigurationCommand(this.model.toDto())
            .execute()
            .always(() => this.spinners.save(false));
    }

    setReadMode(mode: Raven.Client.Http.ReadBalanceBehavior) {
        this.model.readBalanceBehavior(mode);
    }
}

export = clientConfiguration;
