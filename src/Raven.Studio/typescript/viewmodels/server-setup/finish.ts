import setupStep = require("viewmodels/server-setup/setupStep");
import finishSetupCommand = require("commands/setup/finishSetupCommand");

class finish extends setupStep {

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode) {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    finishConfiguration() {
        new finishSetupCommand()
            .execute()
            .done(() => {
                setTimeout(() => this.redirectToStudio(), 3000);
            });
    }

    private redirectToStudio() {
        switch (this.model.mode()) {
            case "Unsecured":
                window.location.href = this.model.unsecureSetup().serverUrl();
                break;
        }

        //TODO: finish me!

    }

}

export = finish;
