import viewModelBase = require("viewmodels/viewModelBase");
import serverSmugglingItem = require("models/resources/serverSmugglingItem");
import serverConnectionInfo = require("models/database/cluster/serverConnectionInfo");
import performSmugglingCommand = require("commands/operations/performSmugglingCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import serverSmugglingLocalStorage = require("common/storage/serverSmugglingLocalStorage");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import databasesManager = require("common/shell/databasesManager");
import eventsCollector = require("common/eventsCollector");

class serverSmuggling extends viewModelBase {
    databases = ko.observableArray<serverSmugglingItem>([]);
    selectedDatabases = ko.observableArray<serverSmugglingItem>();
    targetServer = ko.observable<serverConnectionInfo>(new serverConnectionInfo());

    settingsAccess = new settingsAccessAuthorizer();

    hasDatabases: KnockoutComputed<boolean>;
    noIncremental: KnockoutComputed<boolean>;
    noStripReplication: KnockoutComputed<boolean>;
    noDisableVersioning: KnockoutComputed<boolean>;
    databasesSelection: KnockoutComputed<checkbox>;
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

        var smi = databasesManager.default.databases().map(d => new serverSmugglingItem(d));
        this.databases(smi);

        this.hasDatabases = ko.computed(() => this.databases().length > 0);

        this.noIncremental = ko.computed(() => this.selectedDatabases().length === 0);

        this.noStripReplication = ko.computed(() => {
            var dbs = this.selectedDatabases();
            var replicationCount = dbs.filter(x => x.hasReplicationBundle()).length;
            return dbs.length === 0 || replicationCount === 0;
        });

        this.noDisableVersioning = ko.computed(() => {
            var dbs = this.selectedDatabases();
            var versioningCount = dbs.filter(x => x.hasVersioningBundle()).length;
            return dbs.length === 0 || versioningCount === 0;
        });

        this.databasesSelection = ko.computed(() => {
            var selectedDatabasesCount = this.selectedDatabases().length;
            if (selectedDatabasesCount === this.databases().length)
                return checkbox.Checked;
            if (selectedDatabasesCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.incrementalSelection = ko.computed(() => {
            var databases = this.selectedDatabases();
            if (databases.length === 0)
                return checkbox.UnChecked;

            var incrementalCount = databases.filter(x => x.incremental()).length;
            if (incrementalCount === databases.length)
                return checkbox.Checked;
            if (incrementalCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.stripReplicationSelection = ko.computed(() => {
            var dbs = this.selectedDatabases();
            var replicationCount = dbs.filter(x => x.stripReplicationInformation()).length;
            if (dbs.length === 0 || replicationCount === 0)
                return checkbox.UnChecked;

            var replicationBundleCount = dbs.filter(x => x.hasReplicationBundle()).length;
            if (replicationBundleCount === replicationCount)
                return checkbox.Checked;
            if (replicationBundleCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.disableVersioningSelection = ko.computed(() => {
            var dbs = this.selectedDatabases();
            var versioningCount = dbs.filter(x => x.shouldDisableVersioningBundle()).length;
            if (dbs.length === 0 || versioningCount === 0)
                return checkbox.UnChecked;

            var versioningBundleCount = dbs.filter(x => x.hasVersioningBundle()).length;
            if (versioningBundleCount === versioningCount)
                return checkbox.Checked;
            if (versioningBundleCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.submitEnabled = ko.computed(() => {
            var progress = this.inProgress();
            var selection = this.selectedDatabases().length > 0;
            var url = this.targetServer().url();
            return !progress && selection && !!url;
        });

        this.jsonRequest = ko.computed(() => {
            var request = this.getJson();
            return jsonUtil.syntaxHighlight(request);
        });

        this.curlRequest = ko.computed(() => {
            var json = _.replace(JSON.stringify(this.getJson(), null, 0), /\"/g, "\\\"");

            return "curl -i -H \"Accept: application/json\" -H \"Content-Type: application/json\" -X POST --data \"" + json + "\" " + appUrl.forServer() + "/admin/serverSmuggling";
        });

        this.jsonRequest.subscribe(() => this.saveIntoLocalStorage());

        this.restoreFromLocalStorage();
    }

    activate(args: any) {
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

            // since database might change over time we have to apply saved changes carefully. 
            savedValue.Config.forEach(savedConfig => {
                var item = self.databases().find(r => r.database.name === savedConfig.Name);
                if (item) {
                    self.selectedDatabases.push(item);
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
        if (this.selectedDatabases().length > 0) {
            this.selectedDatabases([]);
        } else {
            this.selectedDatabases(this.databases().slice(0));
        }
    }

    toggleSelectAllIncremental() {
        var databases = this.selectedDatabases();
        if (databases.length === 0)
            return;

        var hasSelected = databases.filter(x => x.incremental()).length > 0;
        for (var i = 0; i < databases.length; i++) {
            databases[i].incremental(!hasSelected);
        }
    }

    toggleSelectAllStripReplication() {
        var databases = this.selectedDatabases();
        var replicationBundleCount = databases.filter(x => x.hasReplicationBundle()).length;
        if (databases.length === 0 || replicationBundleCount === 0)
            return;

        var hasSelected = databases.filter(x => x.stripReplicationInformation()).length > 0;
        for (var i = 0; i < databases.length; i++) {
            if (databases[i].hasReplicationBundle()) {
                databases[i].stripReplicationInformation(!hasSelected);
            }
        }
    }

    toggleSelectAllDisableVersioning() {
        var databases = this.selectedDatabases();
        var versioningBundleCount = databases.filter(x => x.hasVersioningBundle()).length;
        if (databases.length === 0 || versioningBundleCount === 0)
            return;

        var hasSelected = databases.filter(x => x.shouldDisableVersioningBundle()).length > 0;
        for (var i = 0; i < databases.length; i++) {
            if (databases[i].hasVersioningBundle()) {
                databases[i].shouldDisableVersioningBundle(!hasSelected);
            }
        }
    }

    isSelected(item: serverSmugglingItem) {
        return this.selectedDatabases().indexOf(item) >= 0;
    }

    saveIntoLocalStorage() {
        var json = this.getJson();
        json.TargetServer.ApiKey = undefined;
        json.TargetServer.Password = undefined;
        serverSmugglingLocalStorage.setValue(json);
    }

    performMigration() {
        eventsCollector.default.reportEvent("server-smuggling", "execute");

        var request = this.getJson();
        this.messages([]);
        this.inProgress(true);
        this.resultsVisible(true);

        new performSmugglingCommand(request, null, (status) => this.updateProgress(status))
            .execute()
            .always(() => this.inProgress(false));
    }

    private getJson(): serverSmugglingDto {
        var targetServer = this.targetServer().toDto();
        var config = this.selectedDatabases().map(r => r.toDto());
        return {
            TargetServer: targetServer,
            Config: config
        };
    }

    updateProgress(progress: serverSmugglingOperationStateDto) {
        this.messages(progress.Messages);
    }

    toggleJson() {
        eventsCollector.default.reportEvent("server-smuggling", "toggle-json");

        this.showJsonRequest(!this.showJsonRequest());
    }

    toggleCurl() {
        eventsCollector.default.reportEvent("server-smuggling", "toggle-curl");

        this.showCurlRequest(!this.showCurlRequest());
    }

}

export = serverSmuggling;  
