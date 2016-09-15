import viewModelBase = require("viewmodels/viewModelBase");

import endpoints = require("endpoints");
import moment = require("moment");
import copyToClipboard = require("common/copyToClipboard");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

import exportDatabaseModel = require("models/database/tasks/exportDatabaseModel");
import collectionsStats = require("models/database/documents/collectionsStats");

import validateExportDatabaseOptionsCommand = require("commands/database/studio/validateExportDatabaseOptionsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");


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
        const date = moment().format("YYYY-MM-DD HH:mm");
        this.model.exportFileName(`Dump of ${dbName}, ${date}.ravendbdump`);
    }

    private initializeObservables(): void {
        this.filteredCollections = ko.pureComputed(() => {
            const filter = this.filter();
            const collections = this.collections();
            if (!filter) {
                return collections;
            }
            const filterLowerCase = filter.toLowerCase();

            return collections.filter(x => x.toLowerCase().contains(filterLowerCase));
        });

        this.exportCommand = ko.pureComputed<string>(() => {
            var targetServer = appUrl.forServer();
            var model = this.model;
            var outputFilename = exportDatabase.escapeForShell(model.exportFileName());
            var commandTokens = ["Raven.Smuggler", "out", targetServer, outputFilename];

            var types: Array<string> = [];
            if (model.includeDocuments()) {
                types.push("Documents");
            }
            if (model.includeIndexes()) {
                types.push("Indexes");
            }
            if (model.includeTransformers()) {
                types.push("Transformers");
            }
            if (model.removeAnalyzers()) {
                types.push("RemoveAnalyzers");
            }
            if (types.length > 0) {
                commandTokens.push("--operate-on-types=" + types.join(","));
            }

            var databaseName = this.activeDatabase().name;
            commandTokens.push("--database=" + exportDatabase.escapeForShell(databaseName));

            var batchSize = model.batchSize();
            commandTokens.push("--batch-size=" + batchSize);

            if (!model.includeExpiredDocuments()) {
                commandTokens.push("--excludeexpired");
            }

            if (!model.includeAllCollections()) {
                const collections = model.includedCollections();
                commandTokens.push("--metadata-filter=Raven-Entity-Name=" + exportDatabase.escapeForShell(collections.toString()));
            }

            if (model.transformScript()) {
                commandTokens.push("--transform=" + exportDatabase.escapeForShell(model.transformScript()));
            }

            return commandTokens.join(" ");
        });
    }

    copyCommandToClipboard() {
        copyToClipboard.copy(this.exportCommand(), "Command was copied to clipboard.");
    }

    static escapeForShell(input: string) {
        return '"' + input.replace(/[\r\n]/g, "").replace(/(["\\])/g, '\\$1') + '"';
    }

    attached() {
        super.attached();

        $("#transformScriptPopover").popover({
            html: true,
            trigger: "hover",
            content: "Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class=\"code-keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return</span> doc;<br />}</pre>"
        });
    }

    startExport() {
        var db = this.activeDatabase();
        exportDatabase.isExporting(true);

        var exportArg = this.model.toDto();

        new validateExportDatabaseOptionsCommand(exportArg, this.activeDatabase())
            .execute()
            .done(() => {
                var url = endpoints.databases.smuggler.smugglerExport;
                //TODO: pass progress object - or even stop using downloader 
                this.downloader.downloadByPost(db, url, exportArg, exportDatabase.isExporting, ko.observable<string>());
            })
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid export options", response.responseText, response.statusText);
                exportDatabase.isExporting(false);
            });
    }
}

export = exportDatabase;
