import viewModelBase from "viewmodels/viewModelBase";
import validateTwoFactorSecretCommand from "commands/auth/validateTwoFactorSecretCommand";
import getTwoFactorServerConfigurationCommand from "commands/auth/getTwoFactorServerConfigurationCommand";
import requestExecution from "common/notifications/requestExecution";
import protractedCommandsDetector from "common/notifications/protractedCommandsDetector";
import getClientCertificateCommand from "commands/auth/getClientCertificateCommand";

type LimitType = "browser" | "noLimit";

class twoFactorShell extends viewModelBase {
    view = require("views/twoFactorShell.html");

    studioLoadingFakeRequest: requestExecution;

    certificateName = ko.observable<string>();

    focusProceed = ko.observable<boolean>(false);
    
    sessionDurationInMin = ko.observable<number>();
    maxSessionDurationInMin = ko.observable<number>();
    
    code = ko.observable<string>();
    limitType = ko.observable<LimitType>("browser");
    
    constructor() {
        super();

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);
        
        this.code.subscribe(c => {
            if (c?.length === 6) {
                this.focusProceed(true);
            }
        })
        
        this.bindToCurrentInstance("verify");
    }
    
    activate(args: any) {
        super.activate(args);

        const clientCertificateTask = new getClientCertificateCommand()
            .execute()
            .done(clientCert => {
                this.certificateName(clientCert.Name);
            });
        
        const twoFactorConfigTask = new getTwoFactorServerConfigurationCommand()
            .execute()
            .done(response => {
                this.sessionDurationInMin(response.DefaultTwoFactorSessionDurationInMin);
                this.maxSessionDurationInMin(response.MaxTwoFactorSessionDurationInMin);
            });

        return $.when<any>(clientCertificateTask, twoFactorConfigTask);
    }

    compositionComplete() {
        super.compositionComplete();
        $("body").removeClass('loading-active');
        
        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
    }

    verify() {
        if (!this.code() || this.code().length !== 6) {
            return;
        }

        new validateTwoFactorSecretCommand(this.code(), this.limitType() === "browser", this.sessionDurationInMin())
            .execute()
            .done(() => {
                location.href = location.origin;
            });
    }
}

export = twoFactorShell;
