import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import setLicenseLimitsCommand = require("commands/database/cluster/setLicenseLimitsCommand");

class assignCores extends dialogViewModelBase {
    assignedCores = ko.observable<number>();
    availableCores = ko.observable<number>();
    numberOfCores = ko.observable<number>();

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        assignedCores: this.assignedCores
    });

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor(private nodeTag: string, assignedCores: number, availableCores: number, numberOfCores: number) {
        super();

        const maxCoresAccordintToLicense = assignedCores + availableCores;

        this.assignedCores(assignedCores);
        this.availableCores(availableCores);
        this.numberOfCores(numberOfCores);

        this.assignedCores.extend({
            required: true,
            min: 1,
            validation: [
                {
                    validator: (num: number) => num <= this.numberOfCores(),
                    message: "Max cores on node is " + this.numberOfCores()
                },
                {
                    validator: (num: number) => num <= maxCoresAccordintToLicense,
                    message: "Max cores according to license is " + maxCoresAccordintToLicense
                }
            ]
        });
    }

    save() {

        if (!this.isValid(this.validationGroup)) 
            return;

        this.spinners.save(true);

        new setLicenseLimitsCommand(this.nodeTag, this.assignedCores())
            .execute()
            .always(() => {
                this.spinners.save(false);
                this.close();
            });
    }
}

export = assignCores;
