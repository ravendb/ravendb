import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import moment = require("moment");
import copyToClipboard = require("common/copyToClipboard");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import notificationCenter = require("common/notifications/notificationCenter");
import database = require("models/resources/database");
import exportDatabaseModel = require("models/database/tasks/exportDatabaseModel");
import collectionsStats = require("models/database/documents/collectionsStats");
import validateSmugglerOptionsCommand = require("commands/database/studio/validateSmugglerOptionsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import eventsCollector = require("common/eventsCollector");
import popoverUtils = require("common/popoverUtils");
import defaultAceCompleter = require("common/defaultAceCompleter");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import viewHelpers = require("common/helpers/view/viewHelpers");

class exportDatabase extends viewModelBase {

    completer = defaultAceCompleter.completer();
    model = new exportDatabaseModel();

    static isExporting = ko.observable(false);
    isExporting = exportDatabase.isExporting;

    showAdvancedOptions = ko.observable(false);
    showTransformScript = ko.observable(false);

    collections = ko.observableArray<string>();
    filter = ko.observable<string>("");
    filteredCollections: KnockoutComputed<Array<string>>;

    commandTypes: Array<commandLineType> = ["PowerShell", "Cmd", "Bash"];
    effectiveCommandType = ko.observable<commandLineType>("PowerShell");
    effectiveCommand: KnockoutComputed<string>;

    encryptionSection = ko.observable<setupEncryptionKey>();

    constructor() {
        super();
        aceEditorBindingHandler.install();
        
        this.bindToCurrentInstance("customizeConfigurationClicked");

        this.showTransformScript.subscribe(v => {
            if (v) {
                this.model.transformScript(
                    "this.collection = this['@metadata']['@collection'];\r\n" +
                    "// current object is available under 'this' variable\r\n" +
                    "// @change-vector, @id, @last-modified metadata fields are not available");
            } else {
                this.model.transformScript("");
            }
        });
    }

    activate(args: any): void {
        super.activate(args);
        this.updateHelpLink("YD9M1R");

        this.initializeObservables();
        
        const dbName = ko.pureComputed(() => {
            const db = this.activeDatabase();
            return db ? db.name : "";
        });

        this.encryptionSection(setupEncryptionKey.forExport(this.model.encryptionKey, this.model.savedKeyConfirmation, dbName));
        
        this.setupDefaultExportFilename();

        this.encryptionSection().generateEncryptionKey();
        
        this.fetchCollections()
            .done((collections: string[]) => {
                this.collections(collections);
            });
    }

    compositionComplete() {
        super.compositionComplete();

        $('[data-toggle="tooltip"]').tooltip();

        this.encryptionSection().syncQrCode();

        this.model.encryptionKey.subscribe(() => {
            this.encryptionSection().syncQrCode();
            // reset confirmation
            this.model.savedKeyConfirmation(false);
        });
        
        this.model.databaseModel.init();
    }

    private fetchCollections(): JQueryPromise<Array<string>> {
        const collectionsTask = $.Deferred<Array<string>>();

        new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: collectionsStats) => {
                collectionsTask.resolve(stats.collections.map(x => x.name));
            })
            .fail(() => collectionsTask.reject());

        return collectionsTask;
    }

    private setupDefaultExportFilename(): void {
        const dbName = this.activeDatabase().name;
        const date = moment().format("YYYY-MM-DD HH-mm");
        this.model.exportFileName(`Dump of ${dbName} ${date}`);
    }

    private initializeObservables(): void {
        this.filteredCollections = ko.pureComputed(() => {
            const filter = this.filter();
            const collections = this.collections();
            if (!filter) {
                return collections;
            }
            const filterLowerCase = filter.toLowerCase();

            return collections.filter(x => x.toLowerCase().includes(filterLowerCase));
        });

        this.effectiveCommand = ko.pureComputed(() => {
            return this.getCommand(this.effectiveCommandType());
        });
    }

    attached() {
        super.attached();
        
        popoverUtils.longWithHover($("#scriptPopover"),
            {
                content:
                    "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                        "<pre><span class=\"token keyword\">var</span> name = <span class=\"token keyword\">this.</span>FirstName;<br />" +
                        "<span class=\"token keyword\">if</span> (name === <span class=\"token string\">'Bob'</span>)<br />&nbsp;&nbsp;&nbsp;&nbsp;" +
                        "<span class=\"token keyword\">throw </span><span class=\"token string\">'skip'</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                        "<span class=\"token keyword\">this</span>.Freight = <span class=\"token number\">15.3</span>;<br />" +
                        "</pre>"
            });

        popoverUtils.longWithHover($("#js-ongoing-tasks-disabled"), {
            content: "Imported ongoing tasks will be disabled by default."
        });
    }

    customizeConfigurationClicked() {
        this.showAdvancedOptions(true);
        this.model.databaseModel.customizeDatabaseRecordTypes(true);

        setTimeout(() => {
            const $customizeRecord = $(".js-customize-record");
            viewHelpers.animate($customizeRecord, "blink-style");
            
            const topOffset = $customizeRecord.offset().top;
            const container = $customizeRecord.closest(".content-container");
            container.animate({scrollTop: topOffset}, 300);
        }, 200);
    }

    startExport() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        if (this.model.encryptOutput()) {
            if (!this.isValid(this.model.encryptionValidationGroup)) {
                return;
            }
        }
        
        eventsCollector.default.reportEvent("database", "export");

        exportDatabase.isExporting(true);

        const exportArg = this.model.toDto();

        new validateSmugglerOptionsCommand(exportArg, this.activeDatabase())
            .execute()
            .done(() => this.startDownload(exportArg))
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid export options", response.responseText, response.statusText);
                exportDatabase.isExporting(false);
            });
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Could not get next task id.", response.responseText, response.statusText);
                exportDatabase.isExporting(false);
            });
    }

    private startDownload(args: Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide) {
        const $form = $("#exportDownloadForm");
        const db = this.activeDatabase();
        const $downloadOptions = $("[name=DownloadOptions]", $form);

        this.getNextOperationId(db)
            .done((operationId: number) => {
                const url = endpoints.databases.smuggler.smugglerExport;
                const operationPart = "?operationId=" + operationId;
                $form.attr("action", appUrl.forDatabaseQuery(db) + url + operationPart);
                if (!args.TransformScript) {
                    delete args.TransformScript;
                }
                $downloadOptions.val(JSON.stringify(args));
                $form.submit();

                notificationCenter.instance.openDetailsForOperationById(db, operationId);

                notificationCenter.instance.monitorOperation(db, operationId)
                    .fail((exception: Raven.Client.Documents.Operations.OperationExceptionResult) => {
                        messagePublisher.reportError("Could not export database: " + exception.Message, exception.Error, null, false);
                    }).always(() => exportDatabase.isExporting(false));
            });
    }

    getCommandTypeLabel(cmdType: commandLineType) {
        return `Export Command - ${cmdType}`;
    }

    copyCommandToClipboard() {
        const command = this.effectiveCommand();
        copyToClipboard.copy(command, "Export command was copied to clipboard.");
    }
    
    getCommand(commandType: commandLineType) {
        const commandEndpointUrl = (db: database) => appUrl.forServer() + appUrl.forDatabaseQuery(db) + endpoints.databases.smuggler.smugglerExport;

        const db = this.activeDatabase();
        if (!db) {
            return "";
        }

        const args = this.model.toDto();
        if (!args.TransformScript) {
            delete args.TransformScript;
        }

        const fileName = args.FileName;
        delete args.FileName;
        const json = JSON.stringify(args);
        
        switch (commandType) {
            case "PowerShell":
                return `curl.exe -o '${fileName}.ravendbdump' --data DownloadOptions='${json.replace(/"/g, '\\"')}' ${commandEndpointUrl(db)}`;
            case "Cmd":
                return `curl.exe -o "${fileName}.ravendbdump" --data DownloadOptions="${json.replace(/"/g, '\\"')}" ${commandEndpointUrl(db)}`;
            case "Bash":
                return `curl -o '${fileName}.ravendbdump' --data DownloadOptions='${json}' ${commandEndpointUrl(db)}`;
        }
    }
}

export = exportDatabase;
