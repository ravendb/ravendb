import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import eventsCollector = require("common/eventsCollector");
import setLicenseLimitsCommand = require("commands/database/cluster/setLicenseLimitsCommand");

class assignCores extends dialogViewModelBase {
    assignedCores = ko.observable<number>();
    maxUtilizedCores = ko.observable<number | null>();
    availableCores = ko.observable<number>();
    numberOfCores = ko.observable<number>();

    warningText = ko.pureComputed(() => {
        const coresCount = this.assignedCores;
        
        return coresCount() && (coresCount.isValid()) ?
            `${this.pluralize(coresCount(), 'core', 'cores')} will be leased from Cluster License limit` : "";          
    });

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        assignedCores: this.assignedCores
    });

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor(private nodeTag: string, assignedCores: number, maxUtilizedCores: number | null, remainingCoresToAssign: number, numberOfCores: number) {
        super();

        const maxCoresAccordingToLicense = assignedCores + remainingCoresToAssign;

        this.assignedCores(assignedCores);
        this.maxUtilizedCores(maxUtilizedCores);
        this.availableCores(remainingCoresToAssign);
        this.numberOfCores(numberOfCores);

        this.assignedCores.extend({
            required: true,
            min: 1,
            validation: [
                {
                    validator: (num: number) => num <= this.numberOfCores(),
                    message: "Number exceeds max cores on node"
                },
                {
                    validator: (num: number) => num <= maxCoresAccordingToLicense,
                    message: "Number exceeds license limit"
                }
            ]
        });
    }

    save() {
        if (!this.isValid(this.validationGroup)) 
            return;

        eventsCollector.default.reportEvent("assign-cores", "save");
        
        this.spinners.save(true);

        new setLicenseLimitsCommand(this.nodeTag, this.assignedCores(), this.maxUtilizedCores())
            .execute()
            .always(() => {
                this.spinners.save(false);
                this.close();
            });
    }
}

export = assignCores;
