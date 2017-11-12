import setupStep = require("viewmodels/wizard/setupStep");
import finishSetupCommand = require("commands/wizard/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");
import router = require("plugins/router");
import saveUnsecuredSetupCommand = require("commands/wizard/saveUnsecuredSetupCommand");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import checkIfServerIsOnlineCommand = require("commands/wizard/checkIfServerIsOnlineCommand");

class finish extends setupStep {

    configurationTask = $.Deferred<void>();
    completedWithSuccess = ko.observable<boolean>();
    
    spinners = {
        restart: ko.observable<boolean>(false),
        finishing: ko.observable<boolean>(true)
    };
    
    expandSetupLog = ko.observable<boolean>(true);
    showConfigurationLogToggle: KnockoutComputed<boolean>;
    
    currentStep: number;
    
    private websocket: serverNotificationCenterClient;
    
    messages = ko.observableArray<string>([]);
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
        }
        
        this.websocket = new serverNotificationCenterClient();
    }
    
    compositionComplete() {
        super.compositionComplete();

        switch (this.model.mode()) {
            case "Unsecured":
                this.saveUnsecuredConfiguration();
                this.configurationTask.resolve();
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
    
    private saveUnsecuredConfiguration() {
        new saveUnsecuredSetupCommand(this.model.unsecureSetup().toDto())
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
                    this.messages.push(failure.Message);
                    this.messages.push(failure.Error);
                    this.configurationTask.reject();
            }
            
            if (dto) {
                switch (operation.TaskType) {
                    case "Setup":
                        this.messages(dto.Messages);
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
    
    private check() {
        setInterval(() => {
            new checkIfServerIsOnlineCommand(this.model.getStudioUrl())
                .execute()
                .done(() => {
                    this.redirectToStudio();
                });
        }, 500);
    }

    back() {
        switch (this.model.mode()) {
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
