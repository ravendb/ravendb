import viewModelBase = require("viewmodels/viewModelBase");
import getStudioConfigurationCommand = require("commands/resources/getStudioConfigurationCommand");
import databaseStudioConfigurationModel = require("models/database/settings/databaseStudioConfigurationModel");
import eventsCollector = require("common/eventsCollector");
import saveStudioConfigurationCommand = require("commands/resources/saveStudioConfigurationCommand");
import pwaInstaller = require("../../../common/pwaInstaller");

class studioConfiguration extends viewModelBase {

    model: databaseStudioConfigurationModel;
    
    static environments = databaseStudioConfigurationModel.environments;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    pwaInstaller = new pwaInstaller();
    canInstallApp = ko.observable(false);

    activate(args: any) {
        super.activate(args);
        this.canInstallApp(this.pwaInstaller.canInstallApp);

        return new getStudioConfigurationCommand(this.activeDatabase())
            .execute()
            .done((settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) => {
                this.model = settings ? new databaseStudioConfigurationModel(settings) : databaseStudioConfigurationModel.empty();
            });
    }

    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("studio-configuration", "save");

        this.spinners.save(true);

        new saveStudioConfigurationCommand(this.model.toDto(), this.activeDatabase())
            .execute()
            .always(() => this.spinners.save(false));
    }

    installApp() {
        this.pwaInstaller.promptInstallApp()
            .then(result => {
                // If the user said no, then we can't install; hide the prompt.
                if (result.outcome === "dismissed") {
                    this.canInstallApp(false);
                }
            });
    }
}

export = studioConfiguration;
