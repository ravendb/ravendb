import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import saveUnsecuredSetupCommand = require("commands/setup/saveUnsecuredSetupCommand");

class unsecured extends setupStep {

    constructor() {
        super();
        this.bindToCurrentInstance("useDefaultServerUrl");
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "Unsecured") {
            return $.when({ can: true });
        }

        return $.when({redirect: "#welcome" });
    }

    save() {
        //TODO: validate
        //TODO: spininers
        new saveUnsecuredSetupCommand(this.model.unsecureSetup().toDto())
            .execute()
            .done(() => {
                router.navigate("#finish");
            })
    }

    useDefaultServerUrl() {
        this.model.unsecureSetup().serverUrl(location.origin);
    }

}

export = unsecured;
