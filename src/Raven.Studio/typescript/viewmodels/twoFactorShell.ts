import viewModelBase from "viewmodels/viewModelBase";
import validateTwoFactorSecretCommand from "commands/auth/validateTwoFactorSecretCommand";


class twoFactorShell extends viewModelBase {
    view = require("views/twoFactorShell.html");
    
    code = ko.observable<string>();
    withLimits = ko.observable<boolean>(false);
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("verify");
        
        this.withLimits.subscribe(l => {
            console.log("with limits = " + l); //TODO:
        })
    }
    
    verify() {
        if (!this.code() || this.code().length !== 6) {
            return;
        }

        new validateTwoFactorSecretCommand(this.code(), this.withLimits())
            .execute()
            .done(() => {
                location.href = location.origin;
            });
    }
}

export = twoFactorShell;
