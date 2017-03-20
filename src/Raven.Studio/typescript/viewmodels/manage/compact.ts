import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import startDbCompactCommand = require("commands/maintenance/startCompactCommand");
import accessHelper = require("viewmodels/shell/accessHelper");
import databasesManager = require("common/shell/databasesManager");
import eventsCollector = require("common/eventsCollector");

class resourceCompact {
    databaseName = ko.observable<string>('');
    
    databasesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    compactStatusMessages = ko.observableArray<string>();
    compactStatusLastUpdate = ko.observable<string>();

    keepDown = ko.observable<boolean>(false);

    constructor(private parent: compact, private type: string, private resources: KnockoutObservableArray<database>) {
        this.databasesNames = ko.computed(() => resources().map((rs: database) => rs.name));

        this.searchResults = ko.computed(() => {
            var newResourceName = this.databaseName();
            return this.databasesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newDatabaseName = this.databaseName();
            var foundRs = this.resources().find((db: database) => newDatabaseName === db.name);

            if (!foundRs && newDatabaseName.length > 0) {
                errorMessage ="Database name doesn't exist!";
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
    databasesManager = databasesManager.default;

    private dbCompactOptions: resourceCompact = new resourceCompact(this, database.type, this.databasesManager.databases);

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
    }

    startDbCompact() {
        eventsCollector.default.reportEvent("database", "compact");

        this.isBusy(true);
        var self = this;

        new startDbCompactCommand(this.dbCompactOptions.databaseName(), self.dbCompactOptions.updateCompactStatus.bind(self.dbCompactOptions))
            .execute();
    }

}

export = compact;
