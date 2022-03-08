import viewModelBase = require("viewmodels/viewModelBase");
import studioConfigurationGlobalModel = require("models/database/settings/studioConfigurationGlobalModel");
import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");
import jsonUtil = require("common/jsonUtil");
import eventsCollector = require("common/eventsCollector");

class studioConfiguration extends viewModelBase {

    view = require("views/manage/studioConfiguration.html");

    spinners = {
        save: ko.observable<boolean>(false)
    };

    model: studioConfigurationGlobalModel;

    static environments = studioConfigurationGlobalModel.environments;

    activate(args: any) {
        super.activate(args);
     
        return studioSettings.default.globalSettings(true)
            .done((settings: globalSettings) => {
                this.model = new studioConfigurationGlobalModel({
                    Environment: settings.environment.getValue(),
                    Disabled: settings.disabled.getValue(),
                    ReplicationFactor: settings.replicationFactor.getValue(),
                    SendUsageStats: settings.sendUsageStats.getValue(),
                    CollapseDocsWhenOpening: settings.collapseDocsWhenOpening.getValue(),
                    DisableAutoIndexCreation: false
                });

                this.dirtyFlag = new ko.DirtyFlag([
                    this.model.dirtyFlag().isDirty
                ], false, jsonUtil.newLineNormalizingHashFunction);
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

        eventsCollector.default.reportEvent("studio-configuration-global", "save");
        this.spinners.save(true);
        
        studioSettings.default.globalSettings()
            .done(settings => {
                const model = this.model;
                
                settings.environment.setValueLazy(model.environment());
                settings.sendUsageStats.setValueLazy(model.sendUsageStats());
                settings.replicationFactor.setValueLazy(model.replicationFactor());
                settings.collapseDocsWhenOpening.setValue(model.collapseDocsWhenOpening());
                
                settings.save()
                    .done(() => this.model.dirtyFlag().reset())
                    .always(() => this.spinners.save(false));
            })
    }
}

export = studioConfiguration;
