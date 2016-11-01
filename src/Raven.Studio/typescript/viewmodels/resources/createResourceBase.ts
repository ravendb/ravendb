import viewModelBase = require("viewmodels/viewModelBase");
import resource = require("models/resources/resource");
import license = require("models/auth/license");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import resourceCreationModel = require("models/resources/creation/resourceCreationModel");
import forge = require("forge");

abstract class createResourceBase extends dialogViewModelBase {

    abstract getAvailableBundles(): Array<availableBundle>;
    abstract getResourceByName(name: string): resource;

    advancedConfigurationVisible = ko.observable<boolean>(false);
    protected advancedBundleConfigurationVisible = ko.observable<string>();
    showWideDialog: KnockoutComputed<boolean>;

    resourceModel: resourceCreationModel;

    activate() {
        this.initObservables();
        this.resetEncryptionKey();
    }

    protected initObservables() {
        this.showWideDialog = ko.pureComputed(() => {
            const hasAdvancedOpened = this.advancedConfigurationVisible();
            const hasAdvancedBundleOpened = !!this.advancedBundleConfigurationVisible();

            return hasAdvancedBundleOpened || hasAdvancedOpened;
        });

        this.resourceModel.activeBundles.subscribe((changes: Array<KnockoutArrayChange<string>>) => { 
            // hide advanced if respononding bundle was unchecked
            if (!this.advancedBundleConfigurationVisible()) {
                return;
            }
            changes.forEach(change => {
                if (change.status === "deleted" && change.value === this.advancedBundleConfigurationVisible()) {
                    this.advancedBundleConfigurationVisible(null);
                }
            });
        }, null, "arrayChange");

        this.resourceModel.setupValidation((name: string) => !this.getResourceByName(name));
    }

    showAdvancedConfiguration() {
        if (this.advancedConfigurationVisible()) {
            this.advancedConfigurationVisible(false);
            return;
        }

        this.advancedBundleConfigurationVisible(null);
        this.advancedConfigurationVisible(true);
    }

    showAdvancedConfigurationFor(bundleName: string) {
        if (this.advancedBundleConfigurationVisible() === bundleName) {
            this.advancedBundleConfigurationVisible(null);
            return;
        }

        if (!this.resourceModel.activeBundles().contains(bundleName)) {
            this.resourceModel.activeBundles.push(bundleName);
        }
        this.advancedConfigurationVisible(false);
        this.advancedBundleConfigurationVisible(bundleName);
    }

    isBundleActive(name: string): boolean {
        //TODO: implement me!
        return true;
    }

    protected resetEncryptionKey() {
        const rawKey = forge.random.getBytesSync(32);
        const generatedKey = forge.util.encode64(rawKey);
        this.resourceModel.encryption.key(generatedKey);
    }

}

export = createResourceBase;
