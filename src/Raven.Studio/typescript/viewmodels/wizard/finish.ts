import setupStep = require("viewmodels/wizard/setupStep");
import finishSetupCommand = require("commands/wizard/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");
import router = require("plugins/router");
import app = require("durandal/app");
import saveUnsecuredSetupCommand = require("commands/wizard/saveUnsecuredSetupCommand");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import continueClusterConfigurationCommand = require("commands/wizard/continueClusterConfigurationCommand");
import secureInstructions = require("viewmodels/wizard/secureInstructions");

type messageItem = {
    message: string;
    extraClass: string;
}

class finish extends setupStep {

    view = require("views/wizard/finish.html");

    configurationTask: JQueryDeferred<void>;
    completedWithSuccess = ko.observable<boolean>();
    
    spinners = {
        restart: ko.observable<boolean>(false),
        finishing: ko.observable<boolean>(true)
    };
    
    expandSetupLog = ko.observable<boolean>(true);
    showConfigurationLogToggle: KnockoutComputed<boolean>;
    
    currentStep: number;
    
    private websocket: serverNotificationCenterClient;
    
    messages = ko.observableArray<messageItem>([]);
    canRestart = ko.observable<boolean>(false);
    readme = ko.observable<string>();
    configurationState = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    
    constructor() {
        super();
        
        this.showConfigurationLogToggle = ko.pureComputed(() => {
            const isAnySecureOption = this.model.mode() !== "Unsecured";
            const completed = this.completedWithSuccess();
            
            return isAnySecureOption && completed;
        });
    }
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode) {
            return $.when({ can: true });
        } 

        return $.when({ redirect: "#welcome" });
    }
    
    activate(args: any) {
        super.activate(args);

        switch (this.model.mode()) {
            case "Unsecured":
                this.currentStep = 3;
                break;
            case "LetsEncrypt":
                this.currentStep = 5;
                break;
            case "Secured":
                this.currentStep = 4;
                break;
            case "Continue":
                this.currentStep = 3;
                break;
        }
        
        this.websocket = new serverNotificationCenterClient();
    }
    
    compositionComplete() {
        super.compositionComplete();

        this.startConfiguration();
    }
    
    startConfiguration() {
        this.spinners.finishing(true);
        
        this.configurationTask = $.Deferred<void>();

        switch (this.model.mode()) {
            case "Continue":
                this.continueClusterConfiguration(this.model.toContinueSetupDto());
                break;
            case "Unsecured":
                this.saveUnsecuredConfiguration();
                break;
            case "LetsEncrypt":
                this.saveSecuredConfiguration(endpoints.global.setup.setupLetsencrypt, this.model.toSecuredDto());
                break;
            case "Secured":
                this.saveSecuredConfiguration(endpoints.global.setup.setupSecured, this.model.toSecuredDto());
                break;
        }

        this.configurationTask
            .done(() => {
                this.canRestart(true);
                this.completedWithSuccess(true);
                this.expandSetupLog(false);
            })
            .fail(() => {
                this.completedWithSuccess(false);
            })
            .always(() => {
                this.spinners.finishing(false);
            });
    }
    
    private getNextOperationId(): JQueryPromise<number> {
        return new getNextOperationId(null).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
            });
    }

    private continueClusterConfiguration(dto: Raven.Server.Commercial.ContinueSetupInfo) {
        this.getNextOperationId()
            .done((operationId: number) => {
                this.websocket.watchOperation(operationId, e => this.onChange(e));

                new continueClusterConfigurationCommand(operationId, dto)
                    .execute();
            });
    }
    
    private saveUnsecuredConfiguration() {
        new saveUnsecuredSetupCommand(this.model.toUnsecuredDto())
            .execute()
            .done(() => {
                this.configurationTask.resolve();
            })
            .fail(() => this.configurationTask.reject());
    }

    private saveSecuredConfiguration(url: string, dto: Raven.Server.Commercial.SetupInfo) {
        const $form = $("#secureSetupForm");
        const $downloadOptions = $("[name=Options]", $form);

        this.getNextOperationId()
            .done((operationId: number) => {
                const operationPart = "?operationId=" + operationId;
                $form.attr("action", url + operationPart);
                $downloadOptions.val(JSON.stringify(dto));
                $form.submit();
                
                this.websocket.watchOperation(operationId, e => this.onChange(e));
            });
    }
    
    private onChange(operation: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        if (operation.TaskType === "Setup") {
            let dto: Raven.Server.Commercial.SetupProgressAndResult = null;
            
            switch (operation.State.Status) {
                case "Completed":
                    dto = operation.State.Result as Raven.Server.Commercial.SetupProgressAndResult;
                    this.readme(dto.Readme);
                    this.configurationTask.resolve();
                    break;
                case "InProgress":
                    dto = operation.State.Progress as Raven.Server.Commercial.SetupProgressAndResult;
                    break;
                case "Faulted":
                    const failure = operation.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;
                    this.messages.push({ message: failure.Message, extraClass: "text-danger" });
                    this.messages.push({ message: failure.Error, extraClass: "text-danger" });
                    this.configurationTask.reject();
            }
            
            if (dto) {
                switch (operation.TaskType) {
                    case "Setup":
                        this.messages(dto.Messages.map(x => ({ message: x, extraClass: "" })));
                        break;
                }
            }
        }
    }
    
    private finishConfiguration(waitTimeBeforeRedirect: number) {
        new finishSetupCommand()
            .execute()
            .done(() => {
                setTimeout(() => {
                    this.redirectToStudio();
                }, waitTimeBeforeRedirect);
            });
    }
    
    back() {
        switch (this.model.mode()) {
            case "Continue":
                router.navigate("#continue"); 
                break;
            case "Unsecured":
                router.navigate("#unsecured");
                break;
            default:
                router.navigate("#nodes");
        }
    }

    restart() {
        const mode = this.model.mode();
        
        if (mode === "LetsEncrypt" || mode === "Secured") {
            // notify user that generated certificate needs to be installed
            // before redirecting to studio
            app.showBootstrapDialog(new secureInstructions())
                .done((result) => {
                    if (result) {
                        this.spinners.restart(true);
                        this.finishConfiguration(6000);
                    }
                })
        } else {
            this.spinners.restart(true);
            this.finishConfiguration(2000);
        }
    }
    
    private redirectToStudio() {
        window.location.href = this.model.getStudioUrl();
    }
}

export = finish;
