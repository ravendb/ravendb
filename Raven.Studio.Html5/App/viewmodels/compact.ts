import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import resource = require("models/resource");
import filesystem = require("models/filesystem/filesystem");



class resourceCompact {
    resourceName = ko.observable<string>('');
    isBusy = ko.observable<boolean>();
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    constructor(private type: string, private resources: KnockoutObservableArray<resource>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName == rs.name);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = (this.type == database.type ? "Database" : "File system") + " name doesn't exist!";
            }

            return errorMessage;
        });
    }

}
class compactDatabase extends viewModelBase {
    private dbCompactOptions = new resourceCompact(database.type, shell.databases);
    private fsCompactOptions = new resourceCompact(filesystem.type, shell.fileSystems);

    canActivate(args): any {
        return true;
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which != 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which != 13);
    }

    startDbCompact() {
        this.dbCompactOptions.isBusy(true);

        require(["commands/compactDatabaseCommand"], compactDatabaseCommand => {
            var dbToCompact = shell.databases.first((db: database) => db.name == this.dbCompactOptions.resourceName());
            new compactDatabaseCommand(dbToCompact)
                .execute()
                .always(() => this.dbCompactOptions.isBusy(false));
        });
    }

    startFsCompact() {
        this.fsCompactOptions.isBusy(true);

        require(["commands/filesystem/compactFilesystemCommand"], compactFilesystemCommand => {
            var fsToCompact = shell.fileSystems.first((fs: filesystem) => fs.name == this.fsCompactOptions.resourceName());
            new compactFilesystemCommand(fsToCompact)
                .execute()
                .always(() => this.fsCompactOptions.isBusy(false));
        });
    }
}

export = compactDatabase;