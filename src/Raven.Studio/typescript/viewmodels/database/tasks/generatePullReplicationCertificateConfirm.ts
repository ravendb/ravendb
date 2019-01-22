import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class generatePullReplicationCertificateConfirm extends dialogViewModelBase {
    
    validityInYears = ko.observable<number>(1);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        super();
        
        this.validityInYears.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            validityInYears: this.validityInYears
        });
    }
    
    cancel() {
        dialog.close(this);
    }

    generate() {
        if (this.isValid(this.validationGroup)) {
            dialog.close(this, this.validityInYears());    
        }
    }
}

export = generatePullReplicationCertificateConfirm;
