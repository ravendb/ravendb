import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");

class security extends setupStep {
    
    compositionComplete() {
        super.compositionComplete();
        this.model.mode("LetsEncrypt");
        
        this.setupDisableReasons();
    }
    
    clickUnsecured() {
        this.model.mode("Unsecured");
    }

    clickSecured() {
        this.model.mode("Secured");
    }

    clickGenerate() {
        this.model.mode("LetsEncrypt");
    }

    back() {
        router.navigate("#welcome");
    }

    goToNextView() {
        switch (this.model.mode()) {
            case "Unsecured":
                router.navigate("#unsecured");
                break;
            case "Secured":
                router.navigate("#certificate");
                break;
            case "LetsEncrypt":
                router.navigate("#license");
                break;
        }
    }
}

export = security;
