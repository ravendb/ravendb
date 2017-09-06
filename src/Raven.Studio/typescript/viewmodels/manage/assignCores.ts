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

        this.assignedCores(assignedCores);
        this.availableCores(availableCores);
        this.numberOfCores(numberOfCores);

        this.assignedCores.extend({
            required: true,
            min: 1
        });
    }

    save() {
        //TODO: http://issues.hibernatingrhinos.com/issue/RavenDB-8482
        //if (this.isValid(this.validationGroup)) {
        //    return;
        //}

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
