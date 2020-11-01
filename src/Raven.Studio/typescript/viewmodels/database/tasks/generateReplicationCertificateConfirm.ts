import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class generateReplicationCertificateConfirm extends dialogViewModelBase {
    
    validityInMonths = ko.observable<number>(12);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        super();
        
        this.validityInMonths.extend({
            required: true,
            digit: true
        });
        
        this.validationGroup = ko.validatedObservable({
            validityInMonths: this.validityInMonths
        });
    }
    
    cancel() {
        dialog.close(this);
    }

    generate() {
        if (this.isValid(this.validationGroup)) {
            dialog.close(this, this.validityInMonths());
        }
    }
}

export = generateReplicationCertificateConfirm;
