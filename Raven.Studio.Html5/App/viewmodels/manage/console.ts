import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import adminJsScriptCommand = require("commands/maintenance/adminJsScriptCommand");


class consoleJs extends viewModelBase {

    resourceName = ko.observable<string>('');
    isBusy = ko.observable<boolean>();
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;
	responseText = ko.observable<string>();
	script = ko.observable<string>();
	results = ko.observable<string>();

	predefinedSamples = ko.observableArray<consoleJsSampleDto>([]);

	constructor() {
		super();

		this.predefinedSamples.push({ Name: "c1", Code: "this.a = \"b\";" }); //TODO: change me!
		this.predefinedSamples.push({ Name: "c2", Code: "this.a = \"c\";" }); //TODO: change me!
		this.predefinedSamples.push({ Name: "c3", Code: "this.a = \"d\";" }); //TODO: change me!


		aceEditorBindingHandler.install();
        this.resourcesNames = ko.computed(() => shell.databases().map((rs: resource) => rs.name));
        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = shell.databases().first((rs: resource) => newResourceName === rs.name && rs.type === "database");

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = "Database name doesn't exist!";
            }

            return errorMessage;
        });
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