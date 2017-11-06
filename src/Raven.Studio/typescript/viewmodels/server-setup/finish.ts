import setupStep = require("viewmodels/server-setup/setupStep");
import finishSetupCommand = require("commands/setup/finishSetupCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");

class finish extends setupStep {

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode) {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    compositionComplete() {
        super.compositionComplete();

        switch (this.model.mode()) {
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

                /* TODO
                notificationCenter.instance.openDetailsForOperationById(db, operationId);

                notificationCenter.instance.monitorOperation(db, operationId)
                    .fail((exception: Raven.Client.Documents.Operations.OperationExceptionResult) => {
                        messagePublisher.reportError("Could not export database: " + exception.Message, exception.Error, null, false);
                    }).always(() => exportDatabase.isExporting(false));*/
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
