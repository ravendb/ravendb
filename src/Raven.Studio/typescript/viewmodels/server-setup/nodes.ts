import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import nodeInfo = require("models/setup/nodeInfo");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import endpoints = require("endpoints");

class nodes extends setupStep {

    provideCertificates = ko.pureComputed(() => {
        const mode = this.model.mode();
        return mode && mode === "Secured";
    });
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("removeNode");
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }

        return $.when({redirect: "#welcome" });
    }
    
    save() {
        const nodes = this.model.nodes();
        let isValid = true;
        
        nodes.forEach(node => {
            if (!this.isValid(node.validationGroup)) {
                isValid = false;
            }
        });
        
        if (!this.isValid(this.model.nodesValidationGroup)) {
            isValid = false;
        }
        
        if (isValid) {
            const dto = this.model.toSecuredDto();
            
            switch (this.model.mode()) {
                case "LetsEncrypt":
                    //TODO:
                    break;
                case "Secured":
                    this.saveSecuredConfiguration(dto);
            }
            
        }
    }

    private getNextOperationId(): JQueryPromise<number> {
        return new getNextOperationId(null).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
            });
    }
    
    private saveSecuredConfiguration(dto: Raven.Server.Commercial.SetupInfo) {
        const $form = $("#secureSetupForm");
        const db = this.activeDatabase();
        const $downloadOptions = $("[name=Options]", $form);

        this.getNextOperationId()
            .done((operationId: number) => {
                const url = endpoints.global.setup.setupSecured;
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
    
    addNode() {
        this.model.nodes.push(nodeInfo.empty(this.model.useOwnCertificates));
    }

    removeNode(node: nodeInfo) {
        this.model.nodes.remove(node);
    }

}

export = nodes;
