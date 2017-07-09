/// <reference path="../../../../typings/tsd.d.ts"/>

class addClusterNodeModel {
    serverUrl = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        serverUrl: this.serverUrl
    });

    constructor() {
        this.initValidation();
    }

    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });
    }
}

export = addClusterNodeModel;
