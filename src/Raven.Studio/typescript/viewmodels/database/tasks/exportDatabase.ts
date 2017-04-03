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

import validateExportDatabaseOptionsCommand = require("commands/database/studio/validateExportDatabaseOptionsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import eventsCollector = require("common/eventsCollector");
import popoverUtils = require("common/popoverUtils");
import generalUtils = require("common/generalUtils");

class exportDatabase extends viewModelBase {

    model = new exportDatabaseModel();

    static isExporting = ko.observable(false);
    isExporting = exportDatabase.isExporting;

    showAdvancedOptions = ko.observable(false);
    showTransformScript = ko.observable(false);

    collections = ko.observableArray<string>();
    filter = ko.observable<string>("");
    filteredCollections: KnockoutComputed<Array<string>>;

    exportCommand: KnockoutComputed<string>;

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.setupDefaultExportFilename();
        this.showTransformScript.subscribe(v => {
            if (!v) {
                this.model.transformScript("");
            }
        });
    }

    activate(args: any): void {
        super.activate(args);
        this.updateHelpLink("YD9M1R");

        this.initializeObservables();

        this.fetchCollections()
            .done((collections: string[]) => {
                this.collections(collections);
            });
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

        this.exportCommand = ko.pureComputed<string>(() => {
            //TODO: review for smuggler.exe!
            const db = this.activeDatabase();
            if (!db) {
                return "";
            }

            const targetServer = appUrl.forServer();
            const model = this.model;
            const outputFilename = generalUtils.escapeForShell(model.exportFileName());
            const commandTokens = ["Raven.Smuggler", "out", targetServer, outputFilename];

            const databaseName = db.name;
            commandTokens.push("--database=" + generalUtils.escapeForShell(databaseName));

            const types: Array<string> = [];
            if (model.includeDocuments()) {
                types.push("Documents");
            }
            if (model.includeRevisionDocuments()) {
                types.push("RevisionDocuments");
            }
            if (model.includeIndexes()) {
                types.push("Indexes");
            }
            if (model.includeTransformers()) {
                types.push("Transformers");
            }
            if (model.includeIdentities()) {
                types.push("Identities");
            }
            if (types.length > 0) {
                commandTokens.push("--operate-on-types=" + types.join(","));
            }

            if (model.includeExpiredDocuments()) {
                commandTokens.push("--include-expired");
            }

            if (model.removeAnalyzers()) {
                commandTokens.push("--remove-analyzers");
            }

            if (!model.includeAllCollections()) {
                const collections = model.includedCollections();
                commandTokens.push("--metadata-filter=@collection=" + generalUtils.escapeForShell(collections.toString()));
            }

            if (model.transformScript() && this.showTransformScript()) {
                commandTokens.push("--transform=" + generalUtils.escapeForShell(model.transformScript()));
            }

            return commandTokens.join(" ");
        });
    }

    copyCommandToClipboard() {
        copyToClipboard.copy(this.exportCommand(), "Command was copied to clipboard.");
    }

    attached() {
        super.attached();

        $(".use-transform-script small").popover({
            html: true,
            trigger: "hover",
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            content: "Transform scripts are written in JavaScript. <br/>" +
                "Example:<pre><span class=\"token keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">return</span> doc;<br />}</pre>"
        });
    }

    startExport() {
        eventsCollector.default.reportEvent("database", "export");

        exportDatabase.isExporting(true);

        const exportArg = this.model.toDto();

        new validateExportDatabaseOptionsCommand(exportArg, this.activeDatabase())
            .execute()
            .done(() => this.startDownload(exportArg))
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid export options", response.responseText, response.statusText);
                exportDatabase.isExporting(false);
            });
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                exportDatabase.isExporting(false);
            });
    }

    private getAuthToken(db: database): JQueryPromise<singleAuthToken> {
        return new getSingleAuthTokenCommand(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get single auth token for download.", errorThrown);
                exportDatabase.isExporting(false);
            });
    }

    private startDownload(args: Raven.Client.Documents.Smuggler.DatabaseSmugglerOptions) {
        const $form = $("#exportDownloadForm");
        const db = this.activeDatabase();
        const $downloadOptions = $("[name=DownloadOptions]", $form);

        $.when<any>(this.getNextOperationId(db), this.getAuthToken(db))
            .then(([operationId]:[number], [token]:[singleAuthToken]) => {
                const url = endpoints.databases.smuggler.smugglerExport;
                const authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
                const operationPart = "&operationId=" + operationId;
                $form.attr("action", appUrl.forDatabaseQuery(db) + url + authToken + operationPart);
                $downloadOptions.val(JSON.stringify(args, (key, value) => {
                    if (key === "TransformScript" && value === "") {
                        return undefined;
                    }
                    return value;
                }));
                $form.submit();

                notificationCenter.instance.openDetailsForOperationById(db, operationId);

                notificationCenter.instance.monitorOperation(db, operationId)
                    .fail((exception: Raven.Client.Documents.Operations.OperationExceptionResult) => {
                        messagePublisher.reportError("Could not export database: " + exception.Message, exception.Error, null, false);
                    }).always(() => exportDatabase.isExporting(false));

            });
    }
}

export = exportDatabase;
