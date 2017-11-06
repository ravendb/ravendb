import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/setup/claimDomainCommand");
import registrationInfoCommand = require("commands/setup/registrationInfoCommand");

class domain extends setupStep {

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt") { //TODO: + validate previous step
            return $.when({can: true});
        }

        return $.when({redirect: "#welcome"});
    }

    activate(args: any) {
        super.activate(args);

        return new registrationInfoCommand(this.model.license().toDto())
            .execute()
            .done((result) => {
                const domainModel = this.model.domain();

                domainModel.userEmail(result.Email);
                domainModel.availableDomains(Object.keys(result.Domains));
                
                if (domainModel.availableDomains().length === 1) {
                    domainModel.domain(domainModel.availableDomains()[0]);
                }
            });
    }
    
    
    //TODO handle back in view model


    save() {
        const domainModel = this.model.domain();
        this.afterAsyncValidationCompleted(domainModel.validationGroup, () => {
            if (this.isValid(domainModel.validationGroup)) {

                this.claimDomainIfNeeded()
                    .done(() => {
                        router.navigate("#agreement");
                    });
            }
        });
    }
    
    private claimDomainIfNeeded(): JQueryPromise<void> {
        const domainModel = this.model.domain();
        
        if (domainModel.availableDomains().length === 0) {
            const task = $.Deferred<void>();
            new claimDomainCommand(domainModel.domain(), this.model.license().toDto())
                .execute()
                .done(() => task.resolve())
                .fail(() => task.reject());
            
            return task;
        }
        
        return $.when<void>();
    }

}

export = domain;
