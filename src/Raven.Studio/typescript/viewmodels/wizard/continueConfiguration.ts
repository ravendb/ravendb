import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");

class continueConfiguration extends setupStep {

    back() {
        router.navigate("#welcome");
    }
    
    save() {
        if (this.isValid(this.model.continueSetup().validationGroup)) {
            router.navigate("#finish");
        }
    }
}

export = continueConfiguration;
