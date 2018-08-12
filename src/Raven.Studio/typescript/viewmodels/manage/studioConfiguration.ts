import viewModelBase = require("viewmodels/viewModelBase");
import getGlobalStudioConfigurationCommand = require("commands/resources/getGlobalStudioConfigurationCommand");
import studioConfigurationModel = require("models/database/settings/studioConfigurationModel");
import eventsCollector = require("common/eventsCollector");
import saveGlobalStudioConfigurationCommand = require("commands/resources/saveGlobalStudioConfigurationCommand");
import studioSettings = require("common/settings/studioSettings");
import databaseSettings = require("common/settings/databaseSettings");
import globalSettings = require("common/settings/globalSettings");

class studioConfiguration extends viewModelBase {

    model: studioConfigurationModel;

    static environments = studioConfigurationModel.environments;

    activate(args: any) {
        super.activate(args);
     
        return studioSettings.default.globalSettings(true)
            .done((settings: globalSettings) => {
                this.model = new studioConfigurationModel({
                    Environment: settings.environment.getValue(),
                    Disabled: settings.disabled.getValue(),
                    SendUsageStats: settings.sendUsageStats.getValue()
                });
                
                this.bindActions();
            });
    }
    
    private bindActions() {
        this.model.environment.subscribe((newEnvironment) => {
            studioSettings.default.globalSettings()
                .done(settings => {
                    settings.environment.setValue(newEnvironment);
                })
        });
        
        this.model.sendUsageStats.subscribe(sendStats => {
            studioSettings.default.globalSettings().done(settings => {
                settings.sendUsageStats.setValue(sendStats);
            });
        });
    }
}

export = studioConfiguration;
