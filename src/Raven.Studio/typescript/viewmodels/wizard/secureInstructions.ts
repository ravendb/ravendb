import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class secureInstructions extends dialogViewModelBase {

    view = require("views/wizard/secureInstructions.html");

    certificateInstalled = ko.observable<boolean>(undefined);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        super();
        
        this.initValidation();
    }
    
    private initValidation() {
        this.certificateInstalled.extend({
            validation: [
                {
                    validator: (val: boolean) => val,
                    message: "Please confirm that you have installed the client certificate"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            certificateInstalled: this.certificateInstalled
        });
    }
    
    continueSetup() {
        if (this.isValid(this.validationGroup)) {
            dialog.close(this, true);
        }
    }

}

export = secureInstructions;
