import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import counterStorage = require("models/counter/counterStorage");

class createCounterStorage extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    counterStorageName = ko.observable('');
    counterStoragePath = ko.observable('');
    
    counterStorageNameFocus = ko.observable(true);

    private counterStorages = ko.observableArray<counterStorage>();
    private maxNameLength = 200;

    constructor(counterStorages:KnockoutObservableArray<counterStorage>) {
        super();
        this.counterStorages = counterStorages;
    }

    attached() {
        super.attached();
        var inputElement: any = $("#counterStorageName")[0];
        this.counterStorageName.subscribe((newCounterStorageName) => {
            var errorMessage: string = '';
            
            if (this.isCounterStorageNameExists(newCounterStorageName.toLowerCase(), this.counterStorages())) {
                errorMessage = "Database Name Already Exists!";
            }
            else if ((errorMessage = this.CheckName(newCounterStorageName)) != '') { }
            inputElement.setCustomValidity(errorMessage);
        });
        this.subscribeToPath("#databasePath", this.counterStoragePath, "Path");

        this.counterStorageNameFocus(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    cancel() {
        dialog.close(this);
    }

    
    nextOrCreate() {
        var counterStorageName = this.counterStorageName();

        this.creationTaskStarted = true;
        this.creationTask.resolve(this.counterStorageName(), this.counterStoragePath());
        dialog.close(this);
    }
    

    private isCounterStorageNameExists(databaseName: string, counterStorages: counterStorage[]): boolean {
        for (var i = 0; i < counterStorages.length; i++) {
            if (databaseName == counterStorages[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    private CheckName(name: string): string {
        var rg1 = /^[^\\/\*:\?"<>\|]+$/; // forbidden characters \ / * : ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        var message = '';
        if (!$.trim(name)) {
            message = "An empty counter storage name is forbidden for use!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The counter storage length can't exceed " + this.maxNameLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The counter storage name can't contain any of the following characters: \ / * : ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The counter storage name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;
    }

    private subscribeToPath(tag, element, pathName) {
        var inputElement: any = $(tag)[0];
        element.subscribe((path) => {
            var errorMessage: string = this.isPathLegal(path, pathName);
            inputElement.setCustomValidity(errorMessage);
        });
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^\\*:\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = null;

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > 30) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + 30 + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        return errorMessage;
    }
    
  
}

export = createCounterStorage;