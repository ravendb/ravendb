import viewModelBase = require("viewmodels/viewModelBase");
import studioConfigurationModel = require("models/database/settings/studioConfigurationModel");
import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");

class studioConfiguration extends viewModelBase {

    spinners = {
        save: ko.observable<boolean>(false)
    };

    model: studioConfigurationModel;

    static environments = studioConfigurationModel.environments;

    activate(args: any) {
        super.activate(args);
     
        return studioSettings.default.globalSettings(true)
            .done((settings: globalSettings) => {
                this.model = new studioConfigurationModel({
                    Environment: settings.environment.getValue(),
                    Disabled: settings.disabled.getValue(),
                    ReplicationFactor: settings.replicationFactor.getValue(),
                    SendUsageStats: settings.sendUsageStats.getValue(),
                    CollapseDocsWhenOpening: settings.collapseDocsWhenOpening.getValue()
                });
            });
    }

    compositionComplete() {
        super.compositionComplete();
        $('.studio-configuration [data-toggle="tooltip"]').tooltip();
    }
    
    saveConfiguration() {
        this.spinners.save(true);
        
        studioSettings.default.globalSettings()
            .done(settings => {
                const model = this.model;
                
                settings.environment.setValueLazy(model.environment());
                settings.sendUsageStats.setValueLazy(model.sendUsageStats());
                settings.replicationFactor.setValueLazy(model.replicationFactor());
                settings.collapseDocsWhenOpening.setValue(model.collapseDocsWhenOpening());
                
                settings.save()
                    .always(() => this.spinners.save(false));
            })
    }
}

export = studioConfiguration;
