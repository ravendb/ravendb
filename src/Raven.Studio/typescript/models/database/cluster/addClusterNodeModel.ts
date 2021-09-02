/// <reference path="../../../../typings/tsd.d.ts"/>

class addClusterNodeModel {
    serverUrl = ko.observable<string>();
    nodeTag = ko.observable<string>();
    addAsWatcher = ko.observable<boolean>(false);
    maxUtilizedCores = ko.observable<number>(undefined);
    useAvailableCores = ko.observable<boolean>(true);

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        serverUrl: this.serverUrl,
        nodeTag: this.nodeTag,
        maxUtilizedCores: this.maxUtilizedCores
    });

    constructor() {
        this.useAvailableCores.subscribe(newValue => {
            this.maxUtilizedCores.clearError();
            if (!newValue)
                return;
        });

        this.initValidation();
    }

    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });

        this.nodeTag.extend({
            pattern: {
                message: 'Node tag must contain upper case letters only.',
                params: '^[A-Z]+$'
            },
            minLength: 1,
            maxLength: 4,
        });

        this.maxUtilizedCores.extend({
            required: {
                onlyIf: () => !this.useAvailableCores()
            },
            min: 1
        });
    }
}

export = addClusterNodeModel;
