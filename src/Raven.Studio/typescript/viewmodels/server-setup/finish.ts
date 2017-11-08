import setupStep = require("viewmodels/server-setup/setupStep");
import finishSetupCommand = require("commands/setup/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");
import saveUnsecuredSetupCommand = require("commands/setup/saveUnsecuredSetupCommand");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import validateSetupCommand = require("commands/setup/validateSetupCommand");

class finish extends setupStep {

    private websocket: serverNotificationCenterClient;
    
    messages: KnockoutComputed<Array<string>>;
    private configurationMessages = ko.observableArray<string>([]);
    private validationMessages = ko.observableArray<string>([]);

    canRestart = ko.observable<boolean>(false);
    
    configurationState = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    
    constructor() {
        super();
        
        this.messages = ko.pureComputed(() => {
            const configMessages = this.configurationMessages();
            const validationMessages = this.validationMessages();
            
            return configMessages.concat(...validationMessages);
        })
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
            .execute();
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
        if (operation.TaskType === "Setup" || operation.TaskType === "ValidateSetup") {
            
            let dto = null as Raven.Server.Commercial.SetupProgressAndResult;
            
            switch (operation.State.Status) {
                case "Completed":
                    dto = operation.State.Result as Raven.Server.Commercial.SetupProgressAndResult;
                    if (operation.TaskType === "Setup") {
                        this.startValidation(dto);
                    } else {
                        // both setup and validation was completed - we can restart server and start using RavenDB 
                        this.canRestart(true);
                    }
                    break;
                case "InProgress":
                    dto = operation.State.Progress as Raven.Server.Commercial.SetupProgressAndResult;
                    break;
                case "Faulted":
                    const failure = operation.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;
                    
                    const messagesArray = operation.TaskType === "Setup" ? this.configurationMessages : this.validationMessages;
                    messagesArray.push(failure.Message);
                    messagesArray.push(failure.Error);
            }
            
            if (dto) {
                switch (operation.TaskType) {
                    case "Setup":
                        this.configurationMessages(dto.Messages);
                        break;
                    case "ValidateSetup":
                        this.validationMessages(dto.Messages);
                        break;
                }
            }
        }
    }
    
    private startValidation(operationDto: Raven.Server.Commercial.SetupProgressAndResult) {
        //TODO: what should we pass to validate endpoint?
        this.getNextOperationId()
            .done((operationId: number) => {
                new validateSetupCommand(this.model.mode(), operationId, this.model.toSecuredDto())
                    .execute()
                    .done(() => {
                        this.websocket.watchOperation(operationId, e => this.onChange(e));
                    });
            });
    }
    
    private finishConfiguration() {
        new finishSetupCommand()
            .execute()
            .done(() => {
                setTimeout(() => this.redirectToStudio(), 3000);
            });
    }

    restart() {
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
