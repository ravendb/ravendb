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

	targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

	hasAnyResourceSelected: KnockoutComputed<boolean>;
	hasAllResourcesSelected: KnockoutComputed<boolean>;
	hasResources: KnockoutComputed<boolean>;

    constructor() {
		super();
		this.hasAllResourcesSelected = ko.computed(() => this.selectedResources().length === this.resources().length);
		this.hasAnyResourceSelected = ko.computed(() => this.selectedResources().length > 0); 
		this.hasResources = ko.computed(() => {
			return this.resources().count() > 0;
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

	isSelected(item: serverMigrationItem) {
		return this.selectedResources().indexOf(item) >= 0;
	}

	performMigration() {
		var targetServer = this.targetServer().toDto();
		var config = this.selectedResources().map(r => r.toDto());

		var request: serverMigrationDto = {
			TargetServer: targetServer,
			Config: config
		};

		new performMigrationCommand(request, appUrl.getSystemDatabase())
			.execute();
		//TODO: done get operation id and watch for status + update operation log


	}
}

export = serverMigration;  