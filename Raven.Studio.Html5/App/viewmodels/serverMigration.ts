import viewModelBase = require("viewmodels/viewModelBase");
import serverMigrationItem = require("models/serverMigrationItem");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import database = require("models/database");
import serverConnectionInfo = require("models/serverConnectionInfo");
import performMigrationCommand = require("commands/performMigrationCommand");
import appUrl = require("common/appUrl");

class serverMigration extends viewModelBase {

	resources = ko.observableArray<serverMigrationItem>();
	selectedResources = ko.observableArray<serverMigrationItem>();

	inProgress = ko.observable<boolean>(false);
	resultsVisible = ko.observable<boolean>(false);

	targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

	incremental = ko.observable<boolean>(false);
	messages = ko.observableArray<string>([]);

	hasAnyResourceSelected: KnockoutComputed<boolean>;
	hasAllResourcesSelected: KnockoutComputed<boolean>;
	hasResources: KnockoutComputed<boolean>;
	submitEnabled: KnockoutComputed<boolean>;

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
				var smi = databases.map(d => new serverMigrationItem(d));
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

	toggleSelection(item: serverMigrationItem) {
		if (this.isSelected(item)) {
			this.selectedResources.remove(item);
		} else {
			this.selectedResources.push(item);
		}
	}

	isSelected(item: serverMigrationItem) {
		return this.selectedResources().indexOf(item) >= 0;
	}

	performMigration() {
		var targetServer = this.targetServer().toDto();
		var config = this.selectedResources().map(r => r.toDto());
		this.messages([]);

		var request: serverMigrationDto = {
			TargetServer: targetServer,
			Config: config
		};

		this.inProgress(true);
		this.resultsVisible(true);

		new performMigrationCommand(request, appUrl.getSystemDatabase(), (status) => this.updateProgress(status), this.incremental())
			.execute()
			.always(() => this.inProgress(false));
	}

	updateProgress(progress: serverMigrationOperationStateDto) {
		this.messages(progress.Messages);
	}
}

export = serverMigration;  