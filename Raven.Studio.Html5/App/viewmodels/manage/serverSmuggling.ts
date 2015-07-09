import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import serverSmugglingItem = require("models/resources/serverSmugglingItem");
import serverConnectionInfo = require("models/database/cluster/serverConnectionInfo");
import performSmugglingCommand = require("commands/operations/performSmugglingCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");

class serverSmuggling extends viewModelBase {
	resources = ko.observableArray<serverSmugglingItem>([]);
	selectedResources = ko.observableArray<serverSmugglingItem>();
	targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

	hasResources: KnockoutComputed<boolean>;
	noIncremental: KnockoutComputed<boolean>;
	noStripReplication: KnockoutComputed<boolean>;
	noDisableVersioning: KnockoutComputed<boolean>;
	resourcesSelection: KnockoutComputed<checkbox>;
	incrementalSelection: KnockoutComputed<checkbox>;
	stripReplicationSelection: KnockoutComputed<checkbox>;
	disableVersioningSelection: KnockoutComputed<checkbox>;

	showJsonRequest = ko.observable<boolean>(false);
	jsonRequest: KnockoutComputed<string>;

	submitEnabled: KnockoutComputed<boolean>;
	inProgress = ko.observable<boolean>(false);
	resultsVisible = ko.observable<boolean>(false);
	messages = ko.observableArray<string>([]);

    constructor() {
		super();

		var smi = shell.databases().filter(d => d.name !== "<system>").map(d => new serverSmugglingItem(d));
		this.resources(smi);

		this.hasResources = ko.computed(() => this.resources().count() > 0);

	    this.noIncremental = ko.computed(() => this.selectedResources().length === 0);

	    this.noStripReplication = ko.computed(() => {
			var resources = this.selectedResources();
			var replicationCount = resources.filter(x => x.hasReplicationBundle()).length;
		    return resources.length === 0 || replicationCount === 0;
		});

	    this.noDisableVersioning = ko.computed(() => {
			var resources = this.selectedResources();
			var versioningCount = resources.filter(x => x.hasVersioningBundle()).length;
		    return resources.length === 0 || versioningCount === 0;
	    });

		this.resourcesSelection = ko.computed(() => {
		    var selectedResourcesCount = this.selectedResources().length;
		    if (selectedResourcesCount === this.resources().length)
			    return checkbox.Checked;
		    if (selectedResourcesCount > 0)
			    return checkbox.SomeChecked;
		    return checkbox.UnChecked;
	    });

		this.incrementalSelection = ko.computed(() => {
			var resources = this.selectedResources();
			if (resources.length === 0)
			    return checkbox.UnChecked;

			var incrementalCount = resources.filter(x => x.incremental()).length;
		    if (incrementalCount === resources.length)
			    return checkbox.Checked;
		    if (incrementalCount > 0)
			    return checkbox.SomeChecked;
		    return checkbox.UnChecked;
	    });

		this.stripReplicationSelection = ko.computed(() => {
			var resources = this.selectedResources();
			var replicationCount = resources.filter(x => x.stripReplicationInformation()).length;
			if (resources.length === 0 || replicationCount === 0)
			    return checkbox.UnChecked;

			var replicationBundleCount = resources.filter(x => x.hasReplicationBundle()).length;
		    if (replicationBundleCount === replicationCount)
			    return checkbox.Checked;
		    if (replicationBundleCount > 0)
			    return checkbox.SomeChecked;
		    return checkbox.UnChecked;
	    });

		this.disableVersioningSelection = ko.computed(() => {
			var resources = this.selectedResources();
			var versioningCount = resources.filter(x => x.shouldDisableVersioningBundle()).length;
			if (resources.length === 0 || versioningCount === 0)
			    return checkbox.UnChecked;

			var versioningBundleCount = resources.filter(x => x.hasVersioningBundle()).length;
		    if (versioningBundleCount === versioningCount)
			    return checkbox.Checked;
		    if (versioningBundleCount > 0)
			    return checkbox.SomeChecked;
		    return checkbox.UnChecked;
	    });

		this.submitEnabled = ko.computed(() => {
			var progress = this.inProgress();
			var selection = this.selectedResources().length > 0;
			var url = this.targetServer().url();
			return !progress && selection && !!url;
		});

	    this.jsonRequest = ko.computed(() => {
			var request = this.getJson();
		    return jsonUtil.syntaxHighlight(request);
	    });
    }

	toggleSelectAll() {
		if (this.selectedResources().length > 0) {
			this.selectedResources([]);
		} else {
			this.selectedResources(this.resources().slice(0));
		}
	}

	toggleSelectAllIncremental() {
		var resources = this.selectedResources();
		if (resources.length === 0)
			return;

		var hasSelected = resources.filter(x => x.incremental()).length > 0;
		for (var i = 0; i < resources.length; i++) {
			resources[i].incremental(!hasSelected);
		}
	}

	toggleSelectAllStripReplication() {
		var resources = this.selectedResources();
		var replicationBundleCount = resources.filter(x => x.hasReplicationBundle()).length;
		if (resources.length === 0 || replicationBundleCount === 0)
			return;

		var hasSelected = resources.filter(x => x.stripReplicationInformation()).length > 0;
		for (var i = 0; i < resources.length; i++) {
			if (resources[i].hasReplicationBundle()) {
				resources[i].stripReplicationInformation(!hasSelected);
			}
		}
	}

	toggleSelectAllDisableVersioning() {
		var resources = this.selectedResources();
		var versioningBundleCount = resources.filter(x => x.hasVersioningBundle()).length;
		if (resources.length === 0 || versioningBundleCount === 0)
			return;

		var hasSelected = resources.filter(x => x.shouldDisableVersioningBundle()).length > 0;
		for (var i = 0; i < resources.length; i++) {
			if (resources[i].hasVersioningBundle()) {
				resources[i].shouldDisableVersioningBundle(!hasSelected);
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