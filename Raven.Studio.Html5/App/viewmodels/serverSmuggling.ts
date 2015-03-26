import viewModelBase = require("viewmodels/viewModelBase");
import serverSmugglingItem = require("models/serverSmugglingItem");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import database = require("models/database");
import serverConnectionInfo = require("models/serverConnectionInfo");
import performSmugglingCommand = require("commands/performSmugglingCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");

class serverSmuggling extends viewModelBase {

	resources = ko.observableArray<serverSmugglingItem>();
	selectedResources = ko.observableArray<serverSmugglingItem>();

	inProgress = ko.observable<boolean>(false);
	resultsVisible = ko.observable<boolean>(false);

	targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

	incremental = ko.observable<boolean>(false);
	messages = ko.observableArray<string>([]);

	hasAnyResourceSelected: KnockoutComputed<boolean>;
	hasAllResourcesSelected: KnockoutComputed<boolean>;
	hasResources: KnockoutComputed<boolean>;
	submitEnabled: KnockoutComputed<boolean>;

	showJsonRequest = ko.observable<boolean>(false);
	jsonRequest: KnockoutComputed<string>;

    constructor() {
		super();
		this.hasAllResourcesSelected = ko.computed(() => this.selectedResources().length === this.resources().length);
		this.hasAnyResourceSelected = ko.computed(() => this.selectedResources().length > 0); 
		this.hasResources = ko.computed(() => {
			return this.resources().count() > 0;
		});
		this.submitEnabled = ko.computed(() => {
			var progress = this.inProgress();
			var selection = this.hasAnyResourceSelected();
			var url = this.targetServer().url();
			return !progress && selection && !!url;
		});
	    this.jsonRequest = ko.computed(() => {
			var request = this.getJson();
		    return jsonUtil.syntaxHighlight(request);
	    });
    }

    canActivate(args): any {
		var deffered = $.Deferred();

	    this.fetchDatabases()
		    .done(() => deffered.resolve({ can: true }))
		    .fail(() => deffered.resolve({ can: false }));

        return deffered;
	}

	fetchDatabases() {
		return new getDatabasesCommand()
			.execute()
			.done((databases: database[]) => {
				var smi = databases.map(d => new serverSmugglingItem(d));
				this.resources(smi);
		});
	}

    activate(args) {
        super.activate(args);
	}

	toggleSelectAll() {
		if (this.hasAnyResourceSelected()) {
			this.selectedResources([]);
		} else {
			this.selectedResources(this.resources());
		}
	}

	toggleSelection(item: serverSmugglingItem) {
		if (this.isSelected(item)) {
			this.selectedResources.remove(item);
		} else {
			this.selectedResources.push(item);
		}
	}

	isSelected(item: serverSmugglingItem) {
		return this.selectedResources().indexOf(item) >= 0;
	}

	performMigration() {
		var request = this.getJson();
		this.messages([]);
		this.inProgress(true);
		this.resultsVisible(true);

		new performSmugglingCommand(request, appUrl.getSystemDatabase(), (status) => this.updateProgress(status), this.incremental())
			.execute()
			.always(() => this.inProgress(false));
	}

	private getJson(): serverSmugglingDto {
		var targetServer = this.targetServer().toDto();
		var config = this.selectedResources().map(r => r.toDto());
		return {
			TargetServer: targetServer,
			Config: config
		};
	}

	updateProgress(progress: serverSmugglingOperationStateDto) {
		this.messages(progress.Messages);
	}

	toggleJson() {
		this.showJsonRequest(!this.showJsonRequest());
	}
}

export = serverSmuggling;  