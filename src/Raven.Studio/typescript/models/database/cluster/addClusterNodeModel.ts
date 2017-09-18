/// <reference path="../../../../typings/tsd.d.ts"/>

class addClusterNodeModel {
    serverUrl = ko.observable<string>();
    addAsWatcher = ko.observable<boolean>(false);
    assignedCores = ko.observable<number>(undefined);
    usaAvailableCores = ko.observable<boolean>(true);

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        serverUrl: this.serverUrl,
        assignedCores: this.assignedCores
    });

    constructor() {
        this.usaAvailableCores.subscribe(newValue => {
            this.assignedCores.clearError();
            if (!newValue)
                return;

            this.assignedCores(undefined);
        });

        this.initValidation();
    }

    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });

        this.assignedCores.extend({
            required: {
                onlyIf: () => !this.usaAvailableCores()
            },
            min: 1
        });
    }
}

export = addClusterNodeModel;
