import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import getSetupLocalNodeIpsCommand = require("commands/wizard/getSetupLocalNodeIpsCommand");
import getSetupParametersCommand = require("commands/wizard/getSetupParametersCommand");
import genUtils = require("common/generalUtils");
import detectBrowser = require("viewmodels/common/detectBrowser");

class welcome extends setupStep {

    view = require("views/wizard/welcome.html");
   
    disableLetEncrypt = ko.observable<boolean>(false)
    
    browserAlert: detectBrowser;
    
    constructor() {
        super();
        
        this.browserAlert = new detectBrowser(false);
    }
    
    activate(args: any) {
        super.activate(args, { shell: true });
        return $.when<any>(this.fetchLocalNodeIps(), this.fetchSetupParameters())
            .done((localIpsResult, setupParamsResult: [Raven.Server.Commercial.SetupParameters]) => {
                this.model.init(setupParamsResult[0]);

                const ipV4 = localIpsResult[0].filter((ip: string) => ip.split(".").length === 4);
                const ipV6 = localIpsResult[0].filter((x: string) => !ipV4.includes(x));
               
                this.model.localIps(_.uniq(_.concat(["0.0.0.0"], ipV4, ipV6)));
                
                this.model.disableLetsEncrypt(setupParamsResult[0].RunningOnMacOsx);
                
                // Remove localhost IPs if running on Docker
                if (setupParamsResult[0].IsDocker) {
                    this.model.localIps(_.filter(this.model.localIps(), (ip: string) => { return !genUtils.isLocalhostIpAddress(ip); }));
                }
            });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.setupDisableReasons();
    }

    private fetchLocalNodeIps() {
        return new getSetupLocalNodeIpsCommand()
            .execute();
    }
    
    private fetchSetupParameters() {
        return new getSetupParametersCommand() 
            .execute();
    }
    
    setupNewClusterFlow() {
        this.model.mode(this.model.mode() === "Continue" ? "LetsEncrypt" : this.model.mode());
    }

    useSetupPackageFlow() {
        this.model.mode("Continue")
    }

    goToNextView() {
        if (this.model.mode() !== "Continue") {
            router.navigate("#security");
        } else {
            router.navigate("#continue");
        }
    }
}

export = welcome;
