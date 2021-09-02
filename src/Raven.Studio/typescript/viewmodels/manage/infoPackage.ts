import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import notificationCenter = require("common/notifications/notificationCenter");
import viewHelpers = require("common/helpers/view/viewHelpers");
import killOperationCommand = require("commands/operations/killOperationCommand");

class infoPackage extends viewModelBase {
    
    operationId: number;
    
    spinners = {
        abort: ko.observable<boolean>(false),
        clusterWide: ko.observable<boolean>(false),
        serverWide: ko.observable<boolean>(false),
        anyInProgress: null as KnockoutComputed<boolean>
    }
    
    constructor() {
        super();
        
        this.spinners.anyInProgress = ko.pureComputed(() => {
            const cluster = this.spinners.clusterWide();
            const server = this.spinners.serverWide();
            
            return cluster || server;
        })
    }
    
    canDeactivate(isClose: boolean): boolean | JQueryPromise<canDeactivateResultDto> {
        if (this.spinners.anyInProgress()) {
            return this.confirmLeavingPage();
        }
        
        return true;
    }

    private getNextOperationId(): JQueryPromise<number> {
        return new getNextOperationId(null).execute()
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Could not get next task id.", response.responseText, response.statusText);
                this.spinners.serverWide(false);
                this.spinners.clusterWide(false);
            });
    }

    downloadServerWidePackage() {
        this.spinners.serverWide(true);
        eventsCollector.default.reportEvent("info-package", "server-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage);
    }

    downloadClusterWidePackage() {
        this.spinners.clusterWide(true);
        eventsCollector.default.reportEvent("info-package", "cluster-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage);
    }

    private startDownload(url: string) {
        this.getNextOperationId()
            .done((operationId: number) => {
                this.operationId = operationId;
                const operationPart = "?operationId=" + operationId;

                const $form = $("#downloadInfoPackageForm");
                $form.attr("action", appUrl.baseUrl + url + operationPart);
                $("[name=operationId]", $form).val(operationId.toString());
                $form.submit();

                notificationCenter.instance.monitorOperation(null, operationId)
                    .always(() => {
                        this.spinners.clusterWide(false);
                        this.spinners.serverWide(false);
                        this.spinners.abort(false);
                    });
            });
    }

    private confirmLeavingPage() {
        const abort = "Leave and Abort";
        const stay = "Stay on this page";
        const abortResult = $.Deferred<confirmDialogResult>();

        const confirmation = this.confirmationMessage("Abort Debug Package Creation", "Leaving this page will abort package creation.<br>How do you want to proceed?", {
            buttons: [stay, abort],
            forceRejectWithResolve: true,
            html: true
        });

        confirmation.done((result: confirmDialogResult) => abortResult.resolve(result));

        return abortResult;
    }
    
    abortCreatePackage() {
        return viewHelpers.confirmationMessage("Are you sure?", "Do you want to abort package creation?", {
            forceRejectWithResolve: true,
            buttons: ["Cancel", "Abort"]
        })
            .done((result: confirmDialogResult) => {
                if (result.can) {
                    const operationId = this.operationId;

                    new killOperationCommand(null, operationId)
                        .execute()
                        .always(() => {
                            this.spinners.abort(true);
                        });
                }
            });
    }
}

export = infoPackage;
