import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class compactFilesystem extends viewModelBase {

    filesystemName = ko.observable<string>('');
    isBusy = ko.observable<boolean>();
    filesystemNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    constructor() {
        super();

        this.filesystemNames = ko.computed(() => shell.fileSystems().map((fs: filesystem) => fs.name));

        this.searchResults = ko.computed(() => {
            var newFilesystemName = this.filesystemName();
            return this.filesystemNames().filter((name) => name.toLowerCase().indexOf(newFilesystemName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newFilesystemName = this.filesystemName();
            var foundFs = shell.fileSystems.first((fs: filesystem) => newFilesystemName == fs.name);

            if (!foundFs && newFilesystemName.length > 0) {
                errorMessage = "Filesystem name doesn't exist!";
            }

            return errorMessage;
        });
    }

    canActivate(args): any {
        return true;
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which != 13);
    }

    startCompact() {
        this.isBusy(true);

        require(["commands/filesystem/compactFilesystemCommand"], compactFilesystemCommand => {
            var fsToCompact = shell.fileSystems.first((fs: filesystem) => fs.name == this.filesystemName());
            new compactFilesystemCommand(fsToCompact)
                .execute()
                .always(() => this.isBusy(false));
        });
    }
}

export = compactFilesystem;