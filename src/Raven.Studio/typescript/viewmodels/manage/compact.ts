import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import startDbCompactCommand = require("commands/maintenance/startCompactCommand");
import startFsCompactCommand = require("commands/filesystem/startCompactCommand");
import accessHelper = require("viewmodels/shell/accessHelper");
import resourcesManager = require("common/shell/resourcesManager");
import eventsCollector = require("common/eventsCollector");

class resourceCompact {
    resourceName = ko.observable<string>('');
    
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    compactStatusMessages = ko.observableArray<string>();
    compactStatusLastUpdate = ko.observable<string>();

    keepDown = ko.observable<boolean>(false);

    constructor(private parent: compact, private type: string, private resources: KnockoutComputed<resource[]>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName === rs.name);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = (this.type === database.type ? "Database" : "File system") + " name doesn't exist!";
            }

            return errorMessage;
        });
    }

    toggleKeepDown() {
        eventsCollector.default.reportEvent("compact", "keep-down", this.type.toString());

        this.keepDown.toggle();
        this.forceKeepDown();
    }

    forceKeepDown() {
        if (this.keepDown()) {
            var body = document.getElementsByTagName("body")[0];
            body.scrollTop = body.scrollHeight;
        }
    }

    updateCompactStatus(newCompactStatus: compactStatusDto) {
        this.compactStatusMessages(newCompactStatus.Messages);
        this.compactStatusLastUpdate(newCompactStatus.LastProgressMessage);
        this.forceKeepDown();
        this.parent.isBusy(newCompactStatus.State === "Running");
    }

}
class compact extends viewModelBase {
    resourcesManager = resourcesManager.default;

    private dbCompactOptions: resourceCompact = new resourceCompact(this, database.type, this.resourcesManager.databases);
    private fsCompactOptions: resourceCompact = new resourceCompact(this, filesystem.type, this.resourcesManager.fileSystems);

    isBusy = ko.observable<boolean>();
    isForbidden = ko.observable<boolean>();

    canActivate(args: any): any {
        this.isForbidden(accessHelper.isGlobalAdmin() === false);
        return true;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('7HZGOE');
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which !== 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which !== 13);
    }

    startDbCompact() {
        eventsCollector.default.reportEvent("database", "compact");

        this.isBusy(true);
        var self = this;

        new startDbCompactCommand(this.dbCompactOptions.resourceName(), self.dbCompactOptions.updateCompactStatus.bind(self.dbCompactOptions))
            .execute();
    }

    startFsCompact() {
        eventsCollector.default.reportEvent("fs", "compact");

        this.isBusy(true);
        var self = this;

        new startFsCompactCommand(this.fsCompactOptions.resourceName(), self.fsCompactOptions.updateCompactStatus.bind(self.fsCompactOptions))
            .execute();
    }
}

export = compact;
