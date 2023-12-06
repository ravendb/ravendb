import viewModelBase from "viewmodels/viewModelBase";
import validateTwoFactorSecretCommand from "commands/auth/validateTwoFactorSecretCommand";
import getTwoFactorServerConfigurationCommand from "commands/auth/getTwoFactorServerConfigurationCommand";


class twoFactorShell extends viewModelBase {
    view = require("views/twoFactorShell.html");
    
    sessionDurationInMin = ko.observable<number>();
    maxSessionDurationInMin = ko.observable<number>();
    
    code = ko.observable<string>();
    withLimits = ko.observable<boolean>(false);
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("verify");
    }
    
    activate(args: any) {
        super.activate(args);
        
        return new getTwoFactorServerConfigurationCommand()
            .execute()
            .done(response => {
                this.sessionDurationInMin(response.DefaultTwoFactorSessionDurationInMin);
                this.maxSessionDurationInMin(response.MaxTwoFactorSessionDurationInMin);
            });
    }

    verify() {
        if (!this.code() || this.code().length !== 6) {
            return;
        }

        new validateTwoFactorSecretCommand(this.code(), this.withLimits(), this.sessionDurationInMin())
            .execute()
            .done(() => {
                location.href = location.origin;
            });
    }
}

export = twoFactorShell;
