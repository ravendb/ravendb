import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import serverSmugglingItem = require("models/resources/serverSmugglingItem");
import serverConnectionInfo = require("models/database/cluster/serverConnectionInfo");
import performSmugglingCommand = require("commands/operations/performSmugglingCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import serverSmugglingLocalStorage = require("common/serverSmugglingLocalStorage");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class serverSmuggling extends viewModelBase {
    resources = ko.observableArray<serverSmugglingItem>([]);
    selectedResources = ko.observableArray<serverSmugglingItem>();
    targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

    settingsAccess = new settingsAccessAuthorizer();

    hasResources: KnockoutComputed<boolean>;
    noIncremental: KnockoutComputed<boolean>;
    noStripReplication: KnockoutComputed<boolean>;
    noDisableVersioning: KnockoutComputed<boolean>;
    resourcesSelection: KnockoutComputed<checkbox>;
    incrementalSelection: KnockoutComputed<checkbox>;
    stripReplicationSelection: KnockoutComputed<checkbox>;
    disableVersioningSelection: KnockoutComputed<checkbox>;

    showJsonRequest = ko.observable<boolean>(false);
    showCurlRequest = ko.observable<boolean>(false);
    jsonRequest: KnockoutComputed<string>;
    curlRequest: KnockoutComputed<string>;

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

        this.curlRequest = ko.computed(() => {
            var json = JSON.stringify(this.getJson(), null, 0).replaceAll("\"", "\\\"");

            return "curl -i -H \"Accept: application/json\" -H \"Content-Type: application/json\" -X POST --data \"" + json + "\" " + appUrl.forServer() + "/admin/serverSmuggling";
        });

        this.jsonRequest.subscribe(() => this.saveIntoLocalStorage());

        this.restoreFromLocalStorage();
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("MUJQ7G");
    }

    restoreFromLocalStorage() {
        var savedValue = serverSmugglingLocalStorage.get();
        if (savedValue) {
            var self = this;
            var targetServer = this.targetServer();
            targetServer.url(savedValue.TargetServer.Url);
            targetServer.domain(savedValue.TargetServer.Domain);
            targetServer.username(savedValue.TargetServer.Username);
            targetServer.guessCredentialsType();

            // since resources might change over time we have to apply saved changes carefully. 
            savedValue.Config.forEach(savedConfig => {
                var item = self.resources().first(r => r.resource.name === savedConfig.Name);
                if (item) {
                    self.selectedResources.push(item);
                    item.incremental(savedConfig.Incremental);
                    if (item.hasVersioningBundle()) {
                        item.shouldDisableVersioningBundle(savedConfig.ShouldDisableVersioningBundle);
                    }
                    if (item.hasReplicationBundle()) {
                        item.stripReplicationInformation(savedConfig.StripReplicationInformation);
                    }
                }
            });
        }
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

    isSelected(item: serverSmugglingItem) {
        return this.selectedResources().indexOf(item) >= 0;
    }

    saveIntoLocalStorage() {
        var json = this.getJson();
        json.TargetServer.ApiKey = undefined;
        json.TargetServer.Password = undefined;
        serverSmugglingLocalStorage.setValue(json);
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

    toggleCurl() {
        this.showCurlRequest(!this.showCurlRequest());
    }

}

export = serverSmuggling;  
