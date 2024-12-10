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

    proceedHasFocus = ko.observable<boolean>(false);
    
    sessionDurationInMin = ko.observable<number>();
    maxSessionDurationInMin = ko.observable<number>();
    
    code = ko.observable<string>("");
    limitType = ko.observable<LimitType>("browser");
    codeHasFocus = ko.observable<boolean>(false);
    
    constructor() {
        super();

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);
        
        this.code.subscribe(c => {
            if (c?.length === 6) {
                this.proceedHasFocus(true);
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
        
        const container = document.querySelector("body");   

        const keyHandler = (event: KeyboardEvent) => {
            if (event.key === "Backspace") {
                if (this.proceedHasFocus()) {
                    event.preventDefault();
                    event.stopPropagation();
                    this.code(this.code().slice(0, -1));
                    this.codeHasFocus(true);
                }
            }
        }
        
        container.addEventListener("keydown", keyHandler);
        
        this.registerDisposable({
            dispose: () => container.removeEventListener("keydown", keyHandler)
        });
        
        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
        
        this.codeHasFocus(true);
    }

    verify() {
        if (!this.code() || this.code().length !== 6) {
            return;
        }

        new validateTwoFactorSecretCommand(this.code(), this.limitType() === "browser", this.sessionDurationInMin())
            .execute()
            .done(() => {
                location.href = location.origin;
            })
            .fail(() => {
                this.code("");
                this.codeHasFocus(true);
            })
    }
}

export = twoFactorShell;
