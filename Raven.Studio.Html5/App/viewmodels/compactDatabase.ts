import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");

class compactDatabase extends viewModelBase {

    databaseName = ko.observable<string>('');
    isBusy = ko.observable<boolean>();
    databaseNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    constructor() {
        super();

        this.databaseNames = ko.computed(() => shell.databases().map((db: database) => db.name));

        this.searchResults = ko.computed(() => {
            var newDatabaseName = this.databaseName();
            return this.databaseNames().filter((name) => name.toLowerCase().indexOf(newDatabaseName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newDatabaseName = this.databaseName();
            var foundDb = shell.databases.first((db: database) => newDatabaseName == db.name);

            if (!foundDb && newDatabaseName.length > 0) {
                errorMessage = "Database name doesn't exist!";
            }

            return errorMessage;
        });
    }

    canActivate(args): any {
        return true;
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which != 13);
    }

    startCompact() {
        this.isBusy(true);

        require(["commands/compactDatabaseCommand"], compactDatabaseCommand => {
            var dbToCompact = shell.databases.first((db: database) => db.name == this.databaseName());
            new compactDatabaseCommand(dbToCompact)
                .execute()
                .always(() => this.isBusy(false));
        });
    }
}

export = compactDatabase;