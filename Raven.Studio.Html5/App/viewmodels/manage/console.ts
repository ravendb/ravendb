import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import resource = require("models/resources/resource");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import adminJsScriptCommand = require("commands/maintenance/adminJsScriptCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class consoleJs extends viewModelBase {

    resourceName = ko.observable<string>('');
    isBusy = ko.observable<boolean>();
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;
    responseText = ko.observable<string>();
    script = ko.observable<string>();
    results = ko.observable<string>();

    settingsAccess = new settingsAccessAuthorizer();

    predefinedSamples = ko.observableArray<consoleJsSampleDto>([]);

    constructor() {
        super();

        this.predefinedSamples.push({ Name: "Get database stats", Code: "return database.Statistics;" });
        this.predefinedSamples.push({
            Name: "Get configuration values", Code: "return {" +
                "\n	RaiseBatchLimit : database.Configuration.AvailableMemoryForRaisingBatchSizeLimit," +
                "\n	ReduceBatchLimit: database.Configuration.MaxNumberOfItemsToReduceInSingleBatch" +
                "\n};" });
        this.predefinedSamples.push({
            Name: "Change configuration on the fly", Code: "database.Configuration.DisableDocumentPreFetching = true;" +
                "\ndatabase.Configuration.MaxNumberOfItemsToPreFetch = 1024;" +
            "\ndatabase.Configuration.BulkImportBatchTimeout = System.TimeSpan.FromMinutes(13);"
        });
        this.predefinedSamples.push({ Name: "Run idle operations", Code: "database.RunIdleOperations();" });
        this.predefinedSamples.push({
            Name: "Put document",
            Code: "var doc = Raven.Json.Linq.RavenJObject.Parse('{ \"Name\" : \"Raven\" }');" +
            "\nvar metadata = Raven.Json.Linq.RavenJObject.Parse('{ \"Raven-Entity-Name\" : \"Docs\" }');" +
            "\n\ndatabase.Documents.Put('doc/1', null, doc, metadata, null);" });


        aceEditorBindingHandler.install();
        this.resourcesNames = ko.computed(() => shell.databases().map((rs: resource) => rs.name));
        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = shell.databases().first((rs: resource) => newResourceName === rs.name && rs.type === TenantType.Database);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = "Database name doesn't exist!";
            }

            return errorMessage;
        });
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("6BJCAJ");
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress",(e) => e.which != 13);
        $('form :input[name="filesystemName"]').on("keypress",(e) => e.which != 13);
    }

    executeJs() {
        this.isBusy(true);
        new adminJsScriptCommand(this.script(), this.resourceName())
            .execute()
            .done((result) => {
                this.results(JSON.stringify(result, null, 4));
            })
            .always(() => { this.isBusy(false); });
    }

    fillWithSample(sample: consoleJsSampleDto) {
        this.script(sample.Code);
    }
}

export = consoleJs;
