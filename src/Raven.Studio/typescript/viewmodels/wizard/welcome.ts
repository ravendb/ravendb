import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import getSetupLocalNodeIpsCommand = require("commands/wizard/getSetupLocalNodeIpsCommand");
import getSetupParametersCommand = require("commands/wizard/getSetupParametersCommand");
import genUtils = require("common/generalUtils");
import detectBrowser = require("viewmodels/common/detectBrowser");

class welcome extends setupStep {
    
    browserAlert = new detectBrowser(false);

    newFlowSelected = ko.observable<boolean>(true);
    continueFlowSelected = ko.observable<boolean>(false);
    
    activate(args: any) {
        super.activate(args, { shell: true });
        return $.when<any>(this.fetchLocalNodeIps(), this.fetchSetupParameters())
            .done((localIpsResult, setupParamsResult: [Raven.Server.Commercial.SetupParameters]) => {
                this.model.init(setupParamsResult[0]);

                const ipV4 = _.filter(localIpsResult[0], (ip: string) => _.split(ip, '.').length === 4);
                const ipV6 = _.difference(localIpsResult[0], ipV4);
               
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
    
    clickNewFlow() {
        this.newFlowSelected(true);
        this.continueFlowSelected(false);
    }

    clickContinueFlow() {
        this.newFlowSelected(false);
        this.continueFlowSelected(true);
    }

    goToNextView() {
        if (this.newFlowSelected()) {
            router.navigate("#security");
        } else {
            this.model.mode("Continue");
            router.navigate("#continue");
        }
    }
}

export = welcome;
