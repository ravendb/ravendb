import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/setup/claimDomainCommand");


class domain extends setupStep {

    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt") {
            return $.when({ can: true });
        }

        return $.when({redirect: "#welcome"});
    }
    
    activate(args: any) {
        super.activate(args);

        const domainModel = this.model.domain();
        const userInfo = this.model.userDomains();
        if (userInfo) {
            domainModel.userEmail(userInfo.Email);
            domainModel.availableDomains(Object.keys(userInfo.Domains));

            if (domainModel.availableDomains().length === 1) {
                domainModel.domain(domainModel.availableDomains()[0]);
            }
        }
    }

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
        
        if (_.includes(domainModel.availableDomains(), domainModel.domain())) {
            // no need to claim it
            return $.when<void>();
        }

        this.spinners.save(true);
        
        const task = $.Deferred<void>();
        new claimDomainCommand(domainModel.domain(), this.model.license().toDto())
            .execute()
            .done(() => task.resolve())
            .fail(() => task.reject())
            .always(() => this.spinners.save(false));
        
        return task;
        
    }

}

export = domain;
