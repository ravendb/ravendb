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
    showWideDialog: KnockoutComputed<boolean>;

    resourceModel: resourceCreationModel;

    activate() {
        this.initObservables();
    }

    protected initObservables() {
        this.showWideDialog = ko.pureComputed(() => this.advancedConfigurationVisible());
        this.resourceModel.setupValidation((name: string) => !this.getResourceByName(name));
    }

    showAdvancedConfiguration() {
        this.advancedConfigurationVisible.toggle();
    }

    isBundleActive(name: string): boolean {
        //TODO: implement me!
        return true;
    }

}

export = createResourceBase;
