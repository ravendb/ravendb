import viewModelBase = require("viewmodels/viewModelBase");
import studioConfigurationModel = require("models/database/settings/studioConfigurationModel");
import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");
import pwaInstaller = require("common/pwaInstaller");

class studioConfiguration extends viewModelBase {

    spinners = {
        save: ko.observable<boolean>(false)
    };

    model: studioConfigurationModel;

    pwaInstaller = new pwaInstaller();

    canInstallApp = ko.observable(false);

    static environments = studioConfigurationModel.environments;

    activate(args: any) {
        super.activate(args);
        this.canInstallApp(this.pwaInstaller.canInstallApp);
     
        return studioSettings.default.globalSettings(true)
            .done((settings: globalSettings) => {
                this.model = new studioConfigurationModel({
                    Environment: settings.environment.getValue(),
                    Disabled: settings.disabled.getValue(),
                    ReplicationFactor: settings.replicationFactor.getValue(),
                    SendUsageStats: settings.sendUsageStats.getValue()
                });
            });
    }
    
    saveConfiguration() {
        this.spinners.save(true);
        studioSettings.default.globalSettings()
            .done(settings => {
                const model = this.model;
                
                settings.environment.setValueLazy(model.environment());
                settings.sendUsageStats.setValueLazy(model.sendUsageStats());
                settings.replicationFactor.setValueLazy(model.replicationFactor());
                
                settings.save()
                    .always(() => this.spinners.save(false));
            })
    }

    installApp() {
        this.pwaInstaller.promptInstallApp()
            .then(result => {
                // If the user said no, then we can't install until he refreshes the page; hide the prompt.
                // If the user said yes, then the app is installed and we should hide it.
                if (result.outcome === "dismissed" || result.outcome === "accepted") {
                    this.canInstallApp(false);
                }
            });
    }
    
}

export = studioConfiguration;
