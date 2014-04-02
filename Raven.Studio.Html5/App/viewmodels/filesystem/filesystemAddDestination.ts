import app = require("durandal/app");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import commandBase = require("commands/commandBase");

class filesystemAddDestination extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    public destinationUrl = ko.observable('');
    public destinationUrlFocus = ko.observable(true);

    private destinations = ko.observableArray<string>();
    private newCommandBase = new commandBase();

    constructor(destinations) {
        super();
        this.destinations = destinations;
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        this.destinationUrl('http://');
        this.destinationUrlFocus(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    add() {
       
        var destinationUrl = this.destinationUrl();

        if (this.isClientSideInputOK(destinationUrl)) {
            this.creationTaskStarted = true;
            this.creationTask.resolve(destinationUrl);
            dialog.close(this);
        }
    }

    private isClientSideInputOK(destinationUrl): boolean {
        var errorMessage = "";

        if (destinationUrl == null) {
            errorMessage = "Please fill out the Destination url field";
        }
        else if ((errorMessage = this.CheckInput(destinationUrl)) != null) { }

        if (errorMessage != null) {
            this.newCommandBase.reportError(errorMessage);
            this.destinationUrlFocus(true);
            return false;
        }
        return true;
    }

    private CheckInput(url): string {
       
        //hveiras: Modified version I took from uri regex from http://stackoverflow.com/questions/2723140/validating-url-with-jquery-without-the-validate-plugin
        var uriRegex = /(http|https):\/\/(\w+:{ 0, 1}\w*@)?(\S+)(:[0 - 9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?/;

        var message = null;
        if (!uriRegex.test(url)) {
            message = "The destination url is not valid";
        }

        return message;
    }
}

export = filesystemAddDestination;