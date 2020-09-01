import viewModelBase = require("viewmodels/viewModelBase");
import getStudioConfigurationCommand = require("commands/resources/getStudioConfigurationCommand");
import databaseStudioConfigurationModel = require("models/database/settings/databaseStudioConfigurationModel");
import eventsCollector = require("common/eventsCollector");
import saveStudioConfigurationCommand = require("commands/resources/saveStudioConfigurationCommand");
import appUrl = require("common/appUrl");

class studioConfiguration extends viewModelBase {

    model: databaseStudioConfigurationModel;
    serverWideStudioConfigurationUrl = appUrl.forGlobalStudioConfiguration();

    static environments = databaseStudioConfigurationModel.environments;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    activate(args: any) {
        super.activate(args);
        
        return new getStudioConfigurationCommand(this.activeDatabase())
            .execute()
            .done((settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) => {
                this.model = settings ? new databaseStudioConfigurationModel(settings) : databaseStudioConfigurationModel.empty();
            });
    }

    compositionComplete() {
        super.compositionComplete();
        $('.studio-configuration [data-toggle="tooltip"]').tooltip();
    }

    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("studio-configuration", "save");

        this.spinners.save(true);

        new saveStudioConfigurationCommand(this.model.toRemoteDto(), this.activeDatabase())
            .execute()
            .always(() => this.spinners.save(false));
    }
}

export = studioConfiguration;
