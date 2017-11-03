import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import loadAgreementCommand = require("commands/setup/loadAgreementCommand");

class agreement extends setupStep {

    url = ko.observable<string>();
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt" && this.model.domain().userEmail()) { //TODO: + validate previous step
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    activate() {
        return new loadAgreementCommand(this.model.domain().userEmail())
            .execute()
            .done(url => {
                this.url(url);
            });
    }
    
    save() {
        //TODO:
     
    }

}

export = agreement;
