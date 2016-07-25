import viewModelBase = require("viewmodels/viewModelBase");
import resource = require("models/resources/resource");
import license = require("models/auth/license");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createResourceBase extends viewModelBase {
    creationTask = $.Deferred();
    creationTaskStarted = false;

    static resourceName = ko.observable("");
    resourceName = ko.observable("");
    nameCustomValidityError: KnockoutComputed<string>;
    resourcePath = ko.observable("");
    pathCustomValidityError: KnockoutComputed<string>;
    logsPath = ko.observable("");
    logsCustomValidityError: KnockoutComputed<string>;
    resourceTempPath = ko.observable("");
    tempCustomValidityError: KnockoutComputed<string>;

    resourceNameCapitalString = "";
    resourceNameString = "";

    storageEngine = ko.observable("");
    allowVoron = ko.observable<boolean>(true);
    voronWarningVisible = ko.computed(() => !this.allowVoron() && this.storageEngine() === "voron");
    tempPathVisible = ko.computed(() => this.storageEngine() === "voron");
    licenseStatus: KnockoutObservable<licenseStatusDto>;

    constructor(protected resources: KnockoutObservableArray<resource>, protected parent: dialogViewModelBase) {
        super();

        this.licenseStatus = license.licenseStatus;
        this.resourceName = createResourceBase.resourceName;

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newResourceName = this.resourceName();

            if (this.doesCounterStorageNameExist(newResourceName, this.resources())) {
                errorMessage = this.resourceNameCapitalString + " name already exists!";
            }
            else if ((errorMessage = this.checkName(newResourceName)) !== "") { }

            return errorMessage;
        });

        this.pathCustomValidityError = ko.computed(() => {
            var newPath = this.resourcePath();
            var errorMessage: string = this.isPathLegal(newPath, "Path");
            return errorMessage;
        });

        this.logsCustomValidityError = ko.computed(() => {
            var newPath = this.logsPath();
            var errorMessage: string = this.isPathLegal(newPath, "Logs");
            return errorMessage;
        });

        this.tempCustomValidityError = ko.computed(() => {
            var newPath = this.resourceTempPath();
            var errorMessage: string = this.isPathLegal(newPath, "Temp");
            return errorMessage;
        });
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    protected clearResourceName() {
        this.resourceName("");
    }

    isBundleActive(name: string): boolean {
        var licenseStatus: licenseStatusDto = this.licenseStatus();

        if (licenseStatus == null || licenseStatus.IsCommercial === false) {
            return true;
        }
        else {
            var value = (<any>licenseStatus.Attributes)[name];
            return value === "true";
        }
    }

    private doesCounterStorageNameExist(resourceName: string, resources: resource[]): boolean {
        resourceName = resourceName.toLowerCase();
        for (var i = 0; i < resources.length; i++) {
            if (resourceName === resources[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    private checkName(name: string): string {
        var rg1 = /^[^\\/:\*\?"<>\|]+$/; // forbidden characters \ / : * ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var maxLength = 260 - 30;

        var message = "";
        if (!$.trim(name)) {
            message = "Please fill out the " + this.resourceNameString + " name field!";
        }
        else if (name.length > maxLength) {
            message = "The " + this.resourceNameString + " length can't exceed " + maxLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The " + this.resourceNameString + " name can't contain any of the following characters: \ / : * ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The " + this.resourceNameString + " name can't start with a dot!";
        }
                    else if (name[name.length-1] === ".") {
            message = "The " + this.resourceNameString + " name can't end with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;
    }

    protected isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = "";

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > 248) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + 248 + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        return errorMessage;
    }
}

export = createResourceBase;
