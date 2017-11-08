import setupStep = require("viewmodels/server-setup/setupStep");
import finishSetupCommand = require("commands/setup/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");
import router = require("plugins/router");
import saveUnsecuredSetupCommand = require("commands/setup/saveUnsecuredSetupCommand");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");

class finish extends setupStep {

    spinners = {
        restart: ko.observable<boolean>(false)
    };
    
    currentStep: number;
    
    private websocket: serverNotificationCenterClient;
    
    messages = ko.observableArray<string>([]);

    canRestart = ko.observable<boolean>(false);
    
    configurationState = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    
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
                this.currentStep = 6;
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
                break;
            case "LetsEncrypt":
                this.saveSecuredConfiguration(endpoints.global.setup.setupLetsencrypt, this.model.toSecuredDto());
                break;
            case "Secured":
                this.saveSecuredConfiguration(endpoints.global.setup.setupSecured, this.model.toSecuredDto());
                break;
        }
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
                this.canRestart(true);
            })
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
                    this.canRestart(true);
                    break;
                case "InProgress":
                    dto = operation.State.Progress as Raven.Server.Commercial.SetupProgressAndResult;
                    break;
                case "Faulted":
                    const failure = operation.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;
                    this.messages.push(failure.Message);
                    this.messages.push(failure.Error);
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
                setTimeout(() => this.redirectToStudio(), 3000);
            });
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
        switch (this.model.mode()) {
            case "Unsecured":
                window.location.href = this.model.unsecureSetup().serverUrl();
                break;
        }

        //TODO: finish me!

    }

}

export = finish;
