import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");

class welcome extends setupStep {

    chooseUnsecured() {
        this.model.mode("Unsecured");
        this.forwardToNextStep();
    }

    chooseSecured() {
        this.model.mode("Secured");
        this.forwardToNextStep();
    }

    chooseGenerate() {
        this.model.mode("LetsEncrypt");
        this.forwardToNextStep();
    }
    
    forwardToNextStep() {
        switch (this.model.mode()) {
            case "Unsecured":
                router.navigate("#unsecured");
                break;
                //TODO:
        }
    }
    
}

export = welcome;
