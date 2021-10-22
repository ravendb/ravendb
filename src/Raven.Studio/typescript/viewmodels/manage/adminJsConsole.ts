import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import adminJsScriptCommand = require("commands/maintenance/adminJsScriptCommand");
import eventsCollector = require("common/eventsCollector");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import databasesManager = require("common/shell/databasesManager");
import defaultAceCompleter = require("common/defaultAceCompleter");
import { highlight, languages } from "prismjs";

type jsConsolePatchOption = "Server" | "Database";
type consoleJsSampleDto = {
    name: string;
    code: string;
    scope: jsConsolePatchOption;
}

class adminJsModel {
    patchOption = ko.observable<jsConsolePatchOption>("Server");
    selectedDatabase = ko.observable<string>();
    script = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
    }

    private initValidation() {

        this.selectedDatabase.extend({
            required: {
                onlyIf: () => this.patchOption() === "Database"
            }
        });

        this.script.extend({
            required: true,
            aceValidation: true
        });

        this.validationGroup = ko.validatedObservable({
            selectedDatabase: this.selectedDatabase,
            script: this.script
        });
    }
}

class adminJsConsole extends viewModelBase {

    static readonly containerSelector = "#admin-js-console-container";
    static readonly allPatchOptions: Array<jsConsolePatchOption> = ["Server", "Database"];
    static readonly predefinedSamples = ko.observableArray<consoleJsSampleDto>([]);

    view = require("views/manage/adminJsConsole.html");

    model = ko.observable<adminJsModel>();
    executionResult = ko.observable<string>();
    previewItem = ko.observable<consoleJsSampleDto>();
    
    completer = defaultAceCompleter.completer();

    databaseNames: KnockoutComputed<Array<string>>;
    previewCode: KnockoutComputed<string>;
    filteredScripts: KnockoutComputed<Array<consoleJsSampleDto>>;

    filters = {
        searchText: ko.observable<string>()
    };

    spinners = {
        execute: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.createSamples();
        this.initObservables();

        this.model(new adminJsModel());

        this.bindToCurrentInstance("usePatchOption", "useDatabase", "previewScript");
    }

    private initObservables() {
        this.databaseNames = ko.pureComputed(() => databasesManager.default.databases().map((db: database) => db.name));
        this.previewCode = ko.pureComputed(() => {
            const item = this.previewItem();
            if (!item) {
                return "";
            }

            return highlight(item.code, languages.javascript, "js");
        });
        this.filteredScripts = ko.pureComputed(() => {
            let text = this.filters.searchText();

            if (!text) {
                return adminJsConsole.predefinedSamples();
            }

            text = text.toLowerCase();

            return adminJsConsole.predefinedSamples().filter(x => x.name.toLowerCase().includes(text));
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("6BJCAJ");
    }

    attached() {
        super.attached();

        this.createKeyboardShortcut("ctrl+enter", () => this.runScript(), adminJsConsole.containerSelector);
    }

    previewScript(item: consoleJsSampleDto) {
        this.previewItem(item);
    }

    usePatchOption(option: jsConsolePatchOption) {
        this.model().patchOption(option);

        if (option === "Server") {
            this.model().selectedDatabase(null);
        }
    }

    useDatabase(databaseName: string) {
        this.model().selectedDatabase(databaseName);
    }

    useScript() {
        const item = this.previewItem();
        const model = this.model();
        model.script(item.code);
        model.patchOption(item.scope);

        if (item.scope === "Database") {
            model.selectedDatabase(null);
        }

        this.previewItem(null);
    }

    runScript() {
        if (this.isValid(this.model().validationGroup)) {
            eventsCollector.default.reportEvent("console", "execute");

            this.spinners.execute(true);

            new adminJsScriptCommand(this.model().script(), this.model().patchOption() === "Database" ? this.model().selectedDatabase() : undefined)
                .execute()
                .done((response) => {
                    this.executionResult(JSON.stringify(response.Result, null, 4));
                })
                .always(() => this.spinners.execute(false));
        }
    }

    private createSamples() {
        if (adminJsConsole.predefinedSamples.length > 0) {
            // already created
            return;
        }

        /**
         * TODO: update those scripts
         */

        adminJsConsole.predefinedSamples.push({ scope: "Database", name: "Get database stats", code: "return database.Statistics;" });
        adminJsConsole.predefinedSamples.push({
            scope: "Database", name: "Get configuration values", code: "return {" +
            "\n	RaiseBatchLimit : database.Configuration.AvailableMemoryForRaisingBatchSizeLimit," +
            "\n	ReduceBatchLimit: database.Configuration.MaxNumberOfItemsToReduceInSingleBatch" +
            "\n};"
        });
        adminJsConsole.predefinedSamples.push({
            scope: "Database", name: "Change configuration on the fly", code: "database.Configuration.DisableDocumentPreFetching = true;" +
            "\ndatabase.Configuration.MaxNumberOfItemsToPreFetch = 1024;" +
            "\ndatabase.Configuration.BulkImportBatchTimeout = System.TimeSpan.FromMinutes(13);"
        });
        adminJsConsole.predefinedSamples.push({ scope: "Database", name: "Run idle operations", code: "database.RunIdleOperations();" });
        adminJsConsole.predefinedSamples.push({
            scope: "Database", name: "Put document",
            code: "var doc = Raven.Json.Linq.RavenJObject.Parse('{ \"Name\" : \"Raven\" }');" +
            "\nvar metadata = Raven.Json.Linq.RavenJObject.Parse('{ \"@collection\" : \"Docs\" }');" +
            "\n\ndatabase.Documents.Put('doc/1', null, doc, metadata, null);"
        });
    }
}

export = adminJsConsole;
