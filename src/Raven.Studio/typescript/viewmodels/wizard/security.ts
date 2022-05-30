import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import popoverUtils = require("common/popoverUtils");

class security extends setupStep {
    
    compositionComplete() {
        super.compositionComplete();
        this.model.mode("LetsEncrypt");
        
        this.setupDisableReasons();

        popoverUtils.longWithHover($(".toggle-zip-only"), {
            content: `<small>
                          <strong>Toggle ON</strong>: Wizard will only create a setup zip package for external setup. Current server will NOT be modified.<br />
                          <strong>Toggle OFF</strong>: Wizard will create a setup zip package AND set up the current server.
                      </small>`,
            html: true,
            placement: "top"
        })
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
