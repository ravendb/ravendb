import setupStep = require("viewmodels/wizard/setupStep");
import finishSetupCommand = require("commands/wizard/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");
import router = require("plugins/router");
import saveUnsecuredSetupCommand = require("commands/wizard/saveUnsecuredSetupCommand");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import checkIfServerIsOnlineCommand = require("commands/wizard/checkIfServerIsOnlineCommand");
import continueClusterConfigurationCommand = require("commands/wizard/continueClusterConfigurationCommand");

type messageItem = {
    message: string;
    extraClass: string;
}

class finish extends setupStep {

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
        const db = this.activeDatabase();
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
            let dto = null as Raven.Server.Commercial.SetupProgressAndResult;
            
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
    
    private finishConfiguration() {
        new finishSetupCommand()
            .execute()
            .done(() => {
                this.check();
            });
    }
    
    private getUrlForPolling() {
        const serverUrl = this.model.getStudioUrl();

        // poll using http 
        const httpServerUrl = serverUrl.replace("https://", "http://");
        
        // if url has default port use 443 instead since we changed scheme from https -> http
        const url = new URL(httpServerUrl);
        if (!url.port) {
            url.port = "443";
            return url.origin;
        }
        
        return httpServerUrl;
    }
    
    private check() {
        const httpServerUrl = this.getUrlForPolling();
        setInterval(() => {
            new checkIfServerIsOnlineCommand(httpServerUrl)
                .execute()
                .done(() => {
                    this.redirectToStudio();
                })
                .fail((result: JQueryXHR) => {
                    // bad request - we connected to https using http, but server respond
                    // it means it is online
                    if (result.status === 400) {
                        this.redirectToStudio();
                    }
                })
        }, 500);
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
        this.spinners.restart(true);
        this.finishConfiguration();
    }
    
    private redirectToStudio() {
        window.location.href = this.model.getStudioUrl();
    }

}

export = finish;
