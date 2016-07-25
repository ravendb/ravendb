import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import globalConfig = require("viewmodels/manage/globalConfig/globalConfig");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import databaseSetting = require("models/database/cluster/databaseSetting");
import saveClusterConfigurationCommand = require("commands/database/cluster/saveClusterConfigurationCommand");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");

class globalConfigDatabaseSettings extends viewModelBase {
    activated = ko.observable<boolean>(false);

    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;
    databaseSettings = ko.observableArray<databaseSetting>([]);
    isSaveEnabled: KnockoutComputed<boolean>;
    settingsAccess = new settingsAccessAuthorizer();
    loadedClusterConfigurationDto = ko.observable<clusterConfigurationDto>();
    clusterMode = shell.clusterMode;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        if (this.settingsAccess.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            this.fetchClusterConfiguration(null)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(null) }));
        }
        
        return deferred;
    }

    saveChanges() {
        var customSettings: dictionary<string> = {};
        var settings = this.databaseSettings();
        settings.forEach(x => {
            customSettings[x.key()] = x.value();
        });

        new saveClusterConfigurationCommand({
                    EnableReplication: this.loadedClusterConfigurationDto().EnableReplication,
                    DatabaseSettings: customSettings
                }, null)
            .execute()
            .done(() => this.dirtyFlag().reset());
    }

    activateConfig() {
        this.activated(true);
        this.databaseSettings([]);
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for cluster-wide database settings?", "Are you sure?")
            .done(() => {
                this.activated(false);
                this.databaseSettings([]);
                this.saveChanges();
            });
    }

    navigateToCreateCluster() {
        this.navigate(this.appUrls.adminSettingsCluster());
    }

    addNewSetting() {
        this.databaseSettings.push(new databaseSetting(null, null));
    }

    removeSetting(itemToRemove: databaseSetting) {
        this.databaseSettings.remove(itemToRemove);
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("T3P7UA");

        this.dirtyFlag = new ko.DirtyFlag([this.databaseSettings]); 
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchClusterConfiguration(db: database): JQueryPromise<clusterConfigurationDto> {

        var currentConfiguration: JQueryPromise<clusterConfigurationDto> = new
            getDocumentWithMetadataCommand("Raven/Cluster/Configuration", null, true)
            .execute()
            .done((result: clusterConfigurationDto) => {
                this.loadedClusterConfigurationDto(result);
                if (result && result.DatabaseSettings) {
                    for (var key in result.DatabaseSettings) {
                        if (result.DatabaseSettings.hasOwnProperty(key)) {
                            this.databaseSettings.push(new databaseSetting(key, result.DatabaseSettings[key]));
                        }
                    }
                    this.activated(this.databaseSettings().length > 0);
                    this.dirtyFlag().reset();
                }
            });

        return currentConfiguration;
    }

}

export = globalConfigDatabaseSettings;
