import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import registrationInfoCommand = require("commands/setup/registrationInfoCommand");

class license extends setupStep {

    spinners = {
        next: ko.observable<boolean>(false)
    };
    
    licenseUrl = "https://ravendb.net/license/request";

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }
        
        return $.when({ redirect: "#welcome" });
    }

    save() {
        if (this.isValid(this.model.license().validationGroup)) {
            const model = this.model;

            this.spinners.next(true);
            
            switch (model.mode()) {
                case "LetsEncrypt":
                    this.loadRegistrationInfo()
                        .done(() => {
                            router.navigate("#domain");
                        })
                        .always(() => this.spinners.next(false));
                    break;
                case "Secured":
                    // load this even in secured mode - it checks license as side effect
                    this.loadRegistrationInfo()
                        .done(() => {
                            router.navigate("#nodes");
                        })
                        .always(() => this.spinners.next(false));
                    break;
                default:
                    router.navigate("#welcome");
                    break;
            }
        }
    }
    
    private loadRegistrationInfo() {
        return new registrationInfoCommand(this.model.license().toDto())
            .execute()
            .done((result: Raven.Server.Commercial.UserDomainsWithIps) => {
                this.model.userDomains(result);
            });
    }

}

export = license;
