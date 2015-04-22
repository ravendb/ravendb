import viewModelBase = require("viewmodels/viewModelBase");
import serverSmugglingItem = require("models/resources/serverSmugglingItem");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import database = require("models/resources/database");
import serverConnectionInfo = require("models/database/cluster/serverConnectionInfo");
import performSmugglingCommand = require("commands/operations/performSmugglingCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");

class serverSmuggling extends viewModelBase {

	resources = ko.observableArray<serverSmugglingItem>();
	selectedResources = ko.observableArray<serverSmugglingItem>();

	targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

	hasAnyResourceSelected: KnockoutComputed<boolean>;
	hasAllResourcesSelected: KnockoutComputed<boolean>;
	hasResources: KnockoutComputed<boolean>;
	
	hasAllIncremental: KnockoutComputed<boolean>;
	hasAllStripReplication: KnockoutComputed<boolean>;
	hasAllDisableVersioning: KnockoutComputed<boolean>;

	showJsonRequest = ko.observable<boolean>(false);
	jsonRequest: KnockoutComputed<string>;

	submitEnabled: KnockoutComputed<boolean>;
	inProgress = ko.observable<boolean>(false);
	resultsVisible = ko.observable<boolean>(false);
	messages = ko.observableArray<string>([]);

    constructor() {
		super();
		this.hasAllResourcesSelected = ko.computed(() =>  this.selectedResources().length === this.resources().length);
		this.hasAnyResourceSelected = ko.computed(() => this.selectedResources().length > 0); 
		this.hasResources = ko.computed(() => {
			return this.resources().count() > 0;
		});
	    this.hasAllIncremental = ko.computed(() => {
		    var resources = this.resources();
		    if (resources.length === 0)
			    return false;

		    for (var i = 0; i < resources.length; i++) {
			    if (!resources[i].incremental())
				    return false;
		    }
		    return true;
		});
	    this.hasAllStripReplication = ko.computed(() => {
			var resources = this.resources();
			if (resources.length === 0)
				return false;

			for (var i = 0; i < resources.length; i++) {
				if (resources[i].hasReplicationBundle() && !resources[i].stripReplicationInformation())
					return false;
			}
			return true;
		});
	    this.hasAllDisableVersioning = ko.computed(() => {
			var resources = this.resources();
			if (resources.length === 0)
				return false;

			for (var i = 0; i < resources.length; i++) {
				if (resources[i].hasVersioningBundle() && !resources[i].shouldDisableVersioningBundle())
					return false;
			}
			return true;
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
			this.selectedResources(this.resources().slice(0));
		}
	}

	toggleSelectAllIncremental() {
		var resources = this.resources();
		var hasAll = this.hasAllIncremental();
		for (var i = 0; i < resources.length; i++) {
			resources[i].incremental(!hasAll);
		}
	}

	toggleSelectAllStripReplication() {
		var resources = this.resources();
		var hasAll = this.hasAllStripReplication();
		for (var i = 0; i < resources.length; i++) {
			if (resources[i].hasReplicationBundle()) {
				resources[i].stripReplicationInformation(!hasAll);
			}
		}
	}

	toggleSelectAllDisableVersioning() {
		var resources = this.resources();
		var hasAll = this.hasAllDisableVersioning();
		for (var i = 0; i < resources.length; i++) {
			if (resources[i].hasVersioningBundle()) {
				resources[i].shouldDisableVersioningBundle(!hasAll);
			}
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

		new performSmugglingCommand(request, appUrl.getSystemDatabase(), (status) => this.updateProgress(status))
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

	toggleIncremental(item: serverSmugglingItem) {
		if (this.isSelected(item)) {
			item.incremental(!item.incremental());
		}
	}

	toggleStripReplicationInformation(item: serverSmugglingItem) {
		if (this.isSelected(item)) {
			item.stripReplicationInformation(!item.stripReplicationInformation());
		}
	}

	toggleDisableVersioningBundle(item: serverSmugglingItem) {
		if (this.isSelected(item)) {
			item.shouldDisableVersioningBundle(!item.shouldDisableVersioningBundle());
		}
	}
}

export = serverSmuggling;  