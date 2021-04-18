import viewModelBase = require("viewmodels/viewModelBase");
import getStudioConfigurationCommand = require("commands/resources/getStudioConfigurationCommand");
import databaseStudioConfigurationModel = require("models/database/settings/databaseStudioConfigurationModel");
import eventsCollector = require("common/eventsCollector");
import saveStudioConfigurationCommand = require("commands/resources/saveStudioConfigurationCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import accessManager = require("common/shell/accessManager");

class studioConfiguration extends viewModelBase {

    model: databaseStudioConfigurationModel;
    serverWideStudioConfigurationUrl = appUrl.forGlobalStudioConfiguration();
    canNavigateToServerSettings: KnockoutComputed<boolean>;

    static environments = databaseStudioConfigurationModel.environments;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    activate(args: any) {
        super.activate(args);
     
        this.canNavigateToServerSettings = ko.pureComputed(() => {
            return accessManager.default.isClusterAdminOrClusterNodeClearance();
        });
        
        return new getStudioConfigurationCommand(this.activeDatabase())
            .execute()
            .done((settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) => {
                this.model = settings ? new databaseStudioConfigurationModel(settings) : databaseStudioConfigurationModel.empty();
                this.dirtyFlag = new ko.DirtyFlag([
                    this.model.dirtyFlag().isDirty
                ], false, jsonUtil.newLineNormalizingHashFunction);
            });
    }

    saveConfiguration() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("studio-configuration", "save");

        this.spinners.save(true);

        new saveStudioConfigurationCommand(this.model.toRemoteDto(), this.activeDatabase())
            .execute()
            .done(() => {
                this.model.dirtyFlag().reset();
            })
            .always(() => this.spinners.save(false));
    }
}

export = studioConfiguration;
