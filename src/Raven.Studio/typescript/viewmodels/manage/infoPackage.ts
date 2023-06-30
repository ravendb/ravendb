import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import getNextOperationIdCommand = require("commands/database/studio/getNextOperationIdCommand");
import messagePublisher = require("common/messagePublisher");
import notificationCenter = require("common/notifications/notificationCenter");
import viewHelpers = require("common/helpers/view/viewHelpers");
import killOperationCommand = require("commands/operations/killOperationCommand");
import databasesManager = require("common/shell/databasesManager");
import database = require("models/resources/database");
import downloader = require("common/downloader");

type packageScope = "currentServer" | "entireCluster";

class infoPackageModel {
    scope = ko.observable<packageScope>();
    types = ko.observableArray<Raven.Server.Documents.Handlers.Debugging.ServerWideDebugInfoPackageHandler.DebugInfoPackageContentType>(["ServerWide", "Databases", "LogFile"]);
    
    allDatabases = ko.observable<boolean>(true);
    databases = ko.observableArray<string>([]);

    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        this.initValidation();
    }
    
    private initValidation() {
        this.scope.extend({
            required: true
        });

        this.types.extend({
            required: true
        });
        
        this.databases.extend({
            required: {
                onlyIf: () => {
                    const databasesSelected = this.types().indexOf("Databases") !== -1;
                    const allDbs = this.allDatabases();
                    return databasesSelected && !allDbs; 
                }
            }
        });
        
        this.validationGroup = ko.validatedObservable({
            scope: this.scope,
            types: this.types,
            databases: this.databases
        });
    }
    
    toUrlParams() {
        return {
            type: this.types().join(","), // we join it here as it is enum flag, so we want to send to server CSV instead of separate params
            database: this.allDatabases() ? undefined : this.databases()
        }
    }
}

class infoPackage extends viewModelBase {

    view = require("views/manage/infoPackage.html");
    
    model = new infoPackageModel();

    databaseNames: KnockoutComputed<Array<string>>;
    
    operationId: number;
    
    spinners = {
        abort: ko.observable<boolean>(false),
        inProgress: ko.observable<boolean>(false),
    }
    
    constructor() {
        super();
        
        this.databaseNames = ko.pureComputed(() => databasesManager.default.databases().map((db: database) => db.name))
    }
    
    canDeactivate(): boolean | JQueryPromise<canDeactivateResultDto> {
        if (this.spinners.inProgress()) {
            return this.confirmLeavingPage();
        }
        
        return true;
    }

    private getNextOperationId(): JQueryPromise<number> {
        return new getNextOperationIdCommand(null).execute()
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Could not get next task id.", response.responseText, response.statusText);
                this.spinners.inProgress(false);
            });
    }

    // TODO kalczur
    downloadPackage() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        this.spinners.inProgress(true);

        switch (this.model.scope()) {
            case "currentServer":
                this.downloadServerWidePackage();
                break;
            case "entireCluster":
                this.downloadClusterWidePackage();
                break;
            default:
                throw new Error("Unsupported scope: " + this.model.scope());
        }
    }

    private downloadServerWidePackage() {
        eventsCollector.default.reportEvent("info-package", "server-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage);
    }

    private downloadClusterWidePackage() {
        eventsCollector.default.reportEvent("info-package", "cluster-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage);
    }

    // TODO kalczur

    private startDownload(url: string) {
        this.getNextOperationId()
            .done((operationId: number) => {
                this.operationId = operationId;
                
                const urlParams = {
                    ...this.model.toUrlParams(),
                    operationId
                };

                const $form = $("#downloadInfoPackageForm");
                $form.attr("action", appUrl.baseUrl + url);
                downloader.fillHiddenFields(urlParams, $form);
                $form.submit();

                notificationCenter.instance.monitorOperation(null, operationId)
                    .always(() => {
                        this.spinners.inProgress(false);
                        this.spinners.abort(false);
                    });
            });
    }


    // TODO kalczur
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
