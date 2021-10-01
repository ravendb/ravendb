import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import eventsCollector = require("common/eventsCollector");
import setLicenseLimitsCommand = require("commands/database/cluster/setLicenseLimitsCommand");

class assignCores extends dialogViewModelBase {

    view = require("views/manage/assignCores.html");
    
    utilizedCores = ko.observable<number>();
    maxUtilizedCores = ko.observable<number | null>();
    availableCores = ko.observable<number>();
    numberOfCores = ko.observable<number>();

    warningText = ko.pureComputed(() => {
        const maxUtilizedCores = this.maxUtilizedCores();
        
        return maxUtilizedCores && maxUtilizedCores > 0 ?
            `Up to ${this.pluralize(maxUtilizedCores, "core", "cores")} will be leased from Cluster License limit` : "";          
    });

    spinners = {
        save: ko.observable<boolean>(false)
    };

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        maxUtilizedCores: this.maxUtilizedCores
    });

    constructor(private nodeTag: string, utilizedCores: number, maxUtilizedCores: number | null, remainingCoresToAssign: number, numberOfCores: number) {
        super();

        this.utilizedCores(utilizedCores);
        this.maxUtilizedCores(maxUtilizedCores);
        this.availableCores(remainingCoresToAssign);
        this.numberOfCores(numberOfCores);

        this.maxUtilizedCores.extend({
            required: false,
            min: 1,
            max: this.numberOfCores()
        });
    }

    save() {
        if (!this.isValid(this.validationGroup))
            return;

        eventsCollector.default.reportEvent("assign-cores", "save");
        
        this.spinners.save(true);

        new setLicenseLimitsCommand(this.nodeTag, this.maxUtilizedCores())
            .execute()
            .always(() => {
                this.spinners.save(false);
                this.close();
            });
    }
}

export = assignCores;
