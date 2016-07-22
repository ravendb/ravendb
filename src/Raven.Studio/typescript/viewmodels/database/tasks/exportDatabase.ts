import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getCollectionsCommand = require("commands/database/documents/getCollectionsCommand");
import collection = require("models/database/documents/collection");
import validateExportDatabaseOptionsCommand = require("commands/database/studio/validateExportDatabaseOptionsCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");

class filterSetting {
    path = ko.observable<string>("");
    value = ko.observable<string>("");
    shouldMatch = ko.observable<boolean>(false);

    static empty() {
        return new filterSetting();
    }
}

class exportDatabase extends viewModelBase {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
    includeAttachments = ko.observable(false);
    includeExpiredDocuments = ko.observable(false);
    includeAllCollections = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    showAdvancedOptions = ko.observable(false);
    batchSize = ko.observable(1024);
    includedCollections = ko.observableArray<{ collection: string; isIncluded: KnockoutObservable<boolean>; }>();
    filters = ko.observableArray<filterSetting>();
    transformScript = ko.observable<string>();
    exportActionUrl: KnockoutComputed<string>;
    noneDefualtFileName = ko.observable<string>("");
    chooseDifferntFileName = ko.observable<boolean>(false);
    exportCommand: KnockoutComputed<string>;

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('YD9M1R');

        new getCollectionsCommand(this.activeDatabase())
            .execute()
            .done((collections: collection[]) => {
                this.includedCollections(collections.map(c => {
                    return {
                        collection: c.name,
                        isIncluded: ko.observable(false)
                    }
                }));
            });

        this.exportCommand = ko.computed(() => {
            var targetServer = appUrl.forServer();
            var outputFilename = this.chooseDifferntFileName() ? exportDatabase.escapeForShell(this.noneDefualtFileName()) : "raven.dump"; 
            var commandTokens = ["Raven.Smuggler", "out", targetServer, outputFilename];

            var types = [];
            if (this.includeDocuments()) {
                 types.push("Documents");
            }
            if (this.includeIndexes()) {
                types.push("Indexes");
            }
            if (this.includeAttachments()) {
                types.push("Attachments");
            }
            if (this.includeTransformers()) {
                types.push("Transformers");
            }
            if (this.removeAnalyzers()) {
                types.push("RemoveAnalyzers");
            }
            if (types.length > 0) {
                commandTokens.push("--operate-on-types=" + types.join(","));
            }

            var databaseName = this.activeDatabase().name;
            commandTokens.push("--database=" + exportDatabase.escapeForShell(databaseName));

            var batchSize = this.batchSize();
            commandTokens.push("--batch-size=" + batchSize);

            if (!this.includeExpiredDocuments()) {
                commandTokens.push("--excludeexpired");
            }

            if (!this.includeAllCollections()) {
                var collections = this.includedCollections().filter((collection) => collection.isIncluded()).map((collection) => collection.collection);
                commandTokens.push("--metadata-filter=Raven-Entity-Name=" + exportDatabase.escapeForShell(collections.toString()));
            }

            var filters = exportDatabase.convertFiltersToDto(this.filters());
            for (var i = 0; i < filters.length; i++) {
                var filter = filters[i];
                var parameterName = filter.ShouldMatch ? "--filter=" : "--negative-filter=";
                commandTokens.push(parameterName + filter.Path + "=" + exportDatabase.escapeForShell(filter.Values.toString()));
            }

            if (this.transformScript()) {
                commandTokens.push("--transform=" + exportDatabase.escapeForShell(this.transformScript())); 
            }
            
            return commandTokens.join(" ");
        });
    }

    /**
     * Groups filters by key
     */
    private static convertFiltersToDto(filters: filterSetting[]): filterSettingDto[] {
        var output: filterSettingDto[] = [];
        for (var i = 0; i < filters.length; i++) {
            var filter = filters[i];

            var existingOutput = output.first(x => filter.shouldMatch() === x.ShouldMatch && filter.path() === x.Path);
            if (existingOutput) {
                existingOutput.Values.push(filter.value());
            } else {
                output.push({
                    ShouldMatch: filter.shouldMatch(),
                    Path: filter.path(),
                    Values: [filter.value()]
                });
            }
        }
        return output;
    }

    static escapeForShell(input: string) {
        return '"' + input.replace(/[\r\n]/g, "").replace(/(["\\])/g, '\\$1') + '"';
    }

    attached() {
        super.attached();
        $("#transformScriptHelp").popover({
            html: true,
            trigger: 'hover',
            content: "Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class=\"code-keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return</span> doc;<br />}</pre>"
        });
    }

    startExport() {
        var db = this.activeDatabase();
        db.isExporting(true);
        db.exportStatus("");

        var operateOnTypes = 0;
        if (this.includeDocuments()) {
            operateOnTypes += 1;
        }
        if (this.includeIndexes()) {
            operateOnTypes += 2;
        }
        if (this.includeAttachments()) {
            operateOnTypes += 4;
        }
        if (this.includeTransformers()) {
            operateOnTypes += 8;
        }
        if (this.removeAnalyzers()) {
            operateOnTypes += 8000;
        }

        var filtersToSend = exportDatabase.convertFiltersToDto(this.filters());

        if (!this.includeAllCollections()) {
            filtersToSend.push(
                {
                    ShouldMatch: true,
                    Path: "@metadata.Raven-Entity-Name",
                    Values: this.includedCollections().filter((curCol) => curCol.isIncluded()).map((curCol) => curCol.collection)
                });
        }

        var smugglerOptions: smugglerOptionsDto = {
            OperateOnTypes: operateOnTypes,
            BatchSize: this.batchSize(),
            ShouldExcludeExpired: !this.includeExpiredDocuments(),
            Filters: filtersToSend,
            TransformScript: this.transformScript(),
            NoneDefualtFileName: this.noneDefualtFileName()
        };

        new validateExportDatabaseOptionsCommand(smugglerOptions, this.activeDatabase())
            .execute()
            .done(() => {
                var url = "/studio-tasks/exportDatabase";
                this.downloader.downloadByPost(db, url, smugglerOptions,
                    db.isExporting, db.exportStatus);
            })
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid export options", response.responseText, response.statusText);
                db.isExporting(false);
            });

    }

    selectOptions() {
        this.showAdvancedOptions(false);
    }

    selectAdvancedOptions() {
        this.showAdvancedOptions(true);
    }

    private removeFilter(filter: filterSetting) {
        this.filters.remove(filter);
    }

    addFilter() {
        var setting = filterSetting.empty();
        setting.shouldMatch(true);
        this.filters.unshift(setting);
    }

}

export = exportDatabase;
