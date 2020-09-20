import viewModelBase = require("viewmodels/viewModelBase");
import clientConfigurationModel = require("models/database/settings/clientConfigurationModel");
import saveGlobalClientConfigurationCommand = require("commands/resources/saveGlobalClientConfigurationCommand");
import getGlobalClientConfigurationCommand = require("commands/resources/getGlobalClientConfigurationCommand");
import eventsCollector = require("common/eventsCollector");

class clientConfiguration extends viewModelBase {

    model: clientConfigurationModel;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    isSaveEnabled = ko.pureComputed(() => {
        const isDirty = this.model.dirtyFlag().isDirty();
        return isDirty && !this.spinners.save();
    });
    
    activate(args: any) {
        super.activate(args);
        
        this.bindToCurrentInstance("saveConfiguration", "setReadMode");
        
        return new getGlobalClientConfigurationCommand()
            .execute()
            .done((dto) => {
                this.model = new clientConfigurationModel(dto);
                this.dirtyFlag = new ko.DirtyFlag([
                    this.model.dirtyFlag().isDirty
                ], false);
            });
    }
    
    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        eventsCollector.default.reportEvent("client-configuration", "save");
        
        this.spinners.save(true);
        this.model.disabled(this.model.isDefined().length === 0);
        
        new saveGlobalClientConfigurationCommand(this.model.toDto())
            .execute()
            .done(() => this.model.dirtyFlag().reset())
            .always(() => this.spinners.save(false));
    }

    setReadMode(mode: Raven.Client.Http.ReadBalanceBehavior) {
        this.model.readBalanceBehavior(mode);
    }
}

export = clientConfiguration;
