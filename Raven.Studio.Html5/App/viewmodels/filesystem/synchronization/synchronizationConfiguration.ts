import appUrl = require("common/appUrl");

import viewModelBase = require("viewmodels/viewModelBase");
import synchronizationConfig = require("models/filesystem/synchronizationConfig");
import getSynchronizationConfigCommand = require("commands/filesystem/getSynchronizationConfigCommand");
import saveConfigurationCommand = require("commands/filesystem/saveConfigurationCommand");
import configurationKey = require("models/filesystem/configurationKey");

class synchronizationConfiguration extends viewModelBase {

    config = ko.observable<synchronizationConfig>();

    isSaveEnabled: KnockoutComputed<boolean>;
    dirtyFlag = new ko.DirtyFlag([]);

    canActivate(args: any): JQueryPromise<any> {
        super.canActivate(args);

        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            this.fetchConfig()
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forResources() }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("1K9SAA");
        this.dirtyFlag = new ko.DirtyFlag([this.config]);
        this.isSaveEnabled = ko.computed(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    saveChanges() {
        new saveConfigurationCommand(this.activeFilesystem(), new configurationKey(this.activeFilesystem(), "Raven/Synchronization/Config"), this.config().toDto())
            .execute()
            .done(() => this.dirtyFlag().reset());
    }

    fetchConfig(): JQueryPromise<any> {
        return new getSynchronizationConfigCommand(this.activeFilesystem())
            .execute()
            .done(result => this.config(result));
    }
}

export = synchronizationConfiguration;
