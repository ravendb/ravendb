import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import popoverUtils = require("common/popoverUtils");

class unsecured extends setupStep {

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "Unsecured") {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        popoverUtils.longWithHover($("label[for=serverUrl] .icon-info"),
            {
                content: 'The URL which the server should listen to. It can be hostname, ip address or 0.0.0.0:{port}',
            });
    }

    back() {
        router.navigate("#welcome");
    }
    
    save() {
        if (this.isValid(this.model.unsecureSetup().validationGroup)) {
            router.navigate("#finish");
        }
    }

}

export = unsecured;
