import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import configurationKey = require("models/filesystem/configurationKey");
import messagePublisher = require("common/messagePublisher");

class createConfigurationKey extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    public configurationKeyName = ko.observable('');
    public configurationKeyNameFocus = ko.observable(true);
    private keys: configurationKey[];

    constructor(keys: Array<configurationKey>) {
        super();
        this.keys = keys;
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        this.configurationKeyNameFocus(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    create() {
        // For now we're just creating the filesystem.

        var configKeyName = this.configurationKeyName();

        if (this.isClientSideInputOk(configKeyName)) {
            this.creationTaskStarted = true;
            this.creationTask.resolve(configKeyName);
            dialog.close(this);
        }
    }

    private isClientSideInputOk(keyName: string): boolean {
        var errorMessage = "";

        if (keyName == null) {
            errorMessage = "Please fill out the Configuration Key name field";
        }
        else if (this.isConfigurationKeyExists(keyName, this.keys)) {
            errorMessage = "Configuration Key Already Exists!";
        }
        else if ((errorMessage = this.checkInput(keyName)) != null) { }

        if (errorMessage != null) {
            messagePublisher.reportError(errorMessage);
            this.configurationKeyNameFocus(true);
            return false;
        }
        return true;
    }

    private checkInput(name: string): string {
        var message: string = null;
        //not implemented for the moment
        
        return message;
    }

    private isConfigurationKeyExists(keyName: string, configurationKeys: configurationKey[]): boolean {
        for (var i = 0; i < configurationKeys.length; i++) {
            if (keyName == configurationKeys[i].key) {
                return true;
            }
        }
        return false;
    }
}

export = createConfigurationKey; 
