import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import EVENTS = require("common/constants/events");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getPluginsInfoCommand = require("commands/database/debug/getPluginsInfoCommand");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import topology = require("models/database/replication/topology");
import shell = require("viewmodels/shell");
import resourcesManager = require("common/shell/resourcesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");

import databaseCreationModel = require("models/resources/creation/databaseCreationModel");

class createDatabase extends createResourceBase {

    readonly databaseBundles: Array<availableBundle> = [
        {
            displayName: "Compression",
            name: "Compression",
            hasAdvancedConfiguration: false
        },
        {
            displayName: "Encryption",
            name: "Encryption",
            hasAdvancedConfiguration: true
        }
    ];

    bundlesEnabled = {
        encryption: this.isBundleActiveComputed("Encryption")
    }

    resourceModel = new databaseCreationModel();

    indexesPathPlaceholder: KnockoutComputed<string>;

    getResourceByName(name: string): database {
        return resourcesManager.default.getDatabaseByName(name);
    }

    activate() {
        super.activate();

        //TODO: if cluster mode preselect replication bundle
        //TODO: if !!this.licenseStatus() && this.licenseStatus().IsCommercial && this.licenseStatus().Attributes.periodicBackup !== "true" preselect periodic export
        //TODO: fetchClusterWideConfig
        //TODO: fetchCustomBundles
    }

    protected initObservables() {
        super.initObservables();

        this.indexesPathPlaceholder = ko.pureComputed(() => {
            const name = this.resourceModel.name();
            return `~/${name || "{Database Name}"}/Indexes/`;
        });

        this.databaseBundles.forEach(bundle => {
            if (!bundle.hasOwnProperty('validationGroup')) {
                bundle.validationGroup = undefined;
            }
        });

        const encryptionConfig = this.databaseBundles.find(x => x.name === "Encryption");
        encryptionConfig.validationGroup = this.resourceModel.encryptionValidationGroup;
    }

    advancedVisibility = {
        encryption: ko.pureComputed(() => this.advancedBundleConfigurationVisible() === "Encryption")
    }

    getAvailableBundles() {
        //TODO: concat with custom bundles 
        return this.databaseBundles;
    }

    createResource() {
        const globalValid = this.isValid(this.resourceModel.globalValidationGroup);
        const advancedValid = this.isValid(this.resourceModel.advancedValidationGroup);
        const encryptionValid = this.bundlesEnabled.encryption() && this.isValid(this.resourceModel.encryptionValidationGroup);

        const allValid = globalValid && advancedValid && encryptionValid;

        if (allValid) {
            this.createResourceInternal();
        } else {
            if (!advancedValid) {
                if (!this.advancedConfigurationVisible()) {
                    this.showAdvancedConfiguration();
                }
            } else if (!encryptionValid) {
                if (!this.advancedVisibility.encryption()) {
                    this.showAdvancedConfigurationFor("Encryption");   
                }
            }
            //TODO: iterate on invalid sections
        }
    }

    private createResourceInternal() {
        const databaseDocument = this.resourceModel.toDto();
        new createDatabaseCommand(databaseDocument)
            .execute()
            .done(() => {
                ko.postbox.publish(EVENTS.Resource
                    .Created,
                    //TODO: it might be temporary event as we use changes api for notifications about newly created resources. 
                    {
                        qualifier: database.qualifier,
                        name: this.resourceModel.name()
                    } as resourceCreatedEventArgs);

            })
            .always(() => {
                dialog.close(this);
            });

        //TODO: issue requests for additional bundles configuration + show dialog about encryption configuration so user can save this 
    }

    private isBundleActiveComputed(bundleName: string) {
        return ko.pureComputed(() => this.resourceModel.activeBundles().contains(bundleName));
    }

}

export = createDatabase;
