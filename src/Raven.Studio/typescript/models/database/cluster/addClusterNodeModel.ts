/// <reference path="../../../../typings/tsd.d.ts"/>

class addClusterNodeModel {
    serverUrl = ko.observable<string>();
    addAsWatcher = ko.observable<boolean>(false);
    assignedCores = ko.observable<number>(undefined);
    autoAssign = ko.observable<boolean>(true);

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        serverUrl: this.serverUrl,
        assignedCores: this.assignedCores
    });

    constructor() {
        this.autoAssign.subscribe(newValue => {
            this.assignedCores.clearError();
            if (!newValue)
                return;

            this.assignedCores(undefined);
        });

        this.initValidation();
    }

    private initValidation() {
        const urlError = ko.observable<string>();
        this.serverUrl.extend({
            required: true,
            validation: [
                {
                    validator: (nodeUrl: string) => {
                        try {
                            new URL(nodeUrl);
                            return true;
                        } catch (e) {
                            urlError((e as Error).message);
                            return false;
                        }
                    },
                    message: `{0}`,
                    params: urlError
                }
            ]
        });

        this.assignedCores.extend({
            required: {
                onlyIf: () => !this.autoAssign()
            },
            min: 1
        });
    }
}

export = addClusterNodeModel;
