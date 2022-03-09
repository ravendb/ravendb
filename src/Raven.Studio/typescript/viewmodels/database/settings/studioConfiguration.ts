import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStudioConfigurationCommand = require("commands/resources/getDatabaseStudioConfigurationCommand");
import studioConfigurationDatabaseModel = require("models/database/settings/studioConfigurationDatabaseModel");
import eventsCollector = require("common/eventsCollector");
import saveDatabaseStudioConfigurationCommand = require("commands/resources/saveDatabaseStudioConfigurationCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import accessManager = require("common/shell/accessManager");
import popoverUtils = require("common/popoverUtils");

class studioConfiguration extends viewModelBase {

    view = require("views/database/settings/studioConfiguration.html");

    model: studioConfigurationDatabaseModel;
    serverWideStudioConfigurationUrl = appUrl.forGlobalStudioConfiguration();
    canNavigateToServerSettings: KnockoutComputed<boolean>;

    static environments = studioConfigurationDatabaseModel.environments;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    activate(args: any) {
        super.activate(args);
     
        this.canNavigateToServerSettings = accessManager.default.isClusterAdminOrClusterNode;
        
        return new getDatabaseStudioConfigurationCommand(this.activeDatabase())
            .execute()
            .done((settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration) => {
                this.model = settings ? new studioConfigurationDatabaseModel(settings) : studioConfigurationDatabaseModel.empty();
                this.dirtyFlag = new ko.DirtyFlag([
                    this.model.dirtyFlag().isDirty
                ], false, jsonUtil.newLineNormalizingHashFunction);
            });
    }

    compositionComplete() {
        popoverUtils.longWithHover($(".disable-auto-index"),
            {
                content: `<ul class="margin-top margin-top-xs no-padding-left margin-left margin-bottom-xs">
                             <li><small>Toggle on to disable creating new Auto-Indexes when making a <strong>dynamic query</strong>.</small></li>
                             <li><small>Query results will be returned only when a matching Auto-Index already exists.</small></li>
                          </ul>`
            });
    }

    saveConfiguration() {
        eventsCollector.default.reportEvent("studio-configuration-database", "save");

        this.spinners.save(true);

        new saveDatabaseStudioConfigurationCommand(this.model.toRemoteDto(), this.activeDatabase())
            .execute()
            .done(() => {
                this.model.dirtyFlag().reset();
            })
            .always(() => this.spinners.save(false));
    }
}

export = studioConfiguration;
