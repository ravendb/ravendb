import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import messagePublisher = require("common/messagePublisher");
import notificationCenter = require("common/notifications/notificationCenter");

class infoPackage extends viewModelBase {
    
    spinners = {
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
        const $form = $("#downloadInfoPackageForm");
        
        
        this.getNextOperationId()
            .done((operationId: number) => {
                const operationPart = "?operationId=" + operationId;
                $form.attr("action", appUrl.baseUrl + url);
                $("[name=operationId]", $form).val(operationId.toString());
                $form.submit();

                notificationCenter.instance.monitorOperation(null, operationId)
                    .always(() => {
                        this.spinners.clusterWide(false);
                        this.spinners.serverWide(false);
                    });
            });
    }
}

export = infoPackage;
