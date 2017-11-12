import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import getSetupLocalNodeIpsCommand = require("commands/wizard/getSetupLocalNodeIpsCommand");

class welcome extends setupStep {

    activate(args: any) {
        super.activate(args, true);
        return this.fetchLocalNodeIps();
    }

    private fetchLocalNodeIps() {
        new getSetupLocalNodeIpsCommand()
            .execute()
            .done((result: Array<string>) => {
                // todo: make the server endpoint return well defined classes    
                const ipV4 = _.filter(result, ip => _.split(ip,  '.').length === 4);
                const ipV6 = _.difference(result,  ipV4);
                this.model.localIps(_.concat(ipV4, ipV6));               
            });
    }
    
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
            case "Secured":
                router.navigate("#certificate");
                break;
            case "LetsEncrypt":
                router.navigate("#license");
                break;
        }
    }
    
}

export = welcome;
