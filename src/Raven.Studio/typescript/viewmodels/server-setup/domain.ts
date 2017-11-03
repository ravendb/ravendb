import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/setup/claimDomainCommand");

class domain extends setupStep {

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt") { //TODO: + validate previous step
            return $.when({can: true});
        }

        return $.when({redirect: "#welcome"});
    }

    //TODO handle back in view model


    //TODO: handle domain is taken

    save() {
        //TODO: should we claim already claimed domain? 

        /* TODO
        const domain = this.model.domain().domain();
        const license = JSON.parse(this.model.license().license()) as Raven.Server.Commercial.License;
        new claimDomainCommand(domain, license)
            .execute();
            //TODO: post exectuion callbacks
            */
        router.navigate("#agreement");
        
        

    }

}

export = domain;
