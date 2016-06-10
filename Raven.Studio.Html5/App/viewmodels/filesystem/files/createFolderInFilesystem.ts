import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class createFolderInFilesystem extends dialogViewModelBase {

    creationTask = $.Deferred();
    creationTaskStarted = false;

    folderName = ko.observable('');

    private foldersLowerCase: string[];

    folderNameCustomValidityError: KnockoutObservable<string>;

    constructor(folders: string[]) {
        super();
        this.foldersLowerCase = folders.map(x => x.toLowerCase());

        this.folderNameCustomValidityError = ko.computed(() => {
            var errorMessage = '';

            if (this.folderExists(this.folderName())) {
                return 'Folder already exists!';
            }

            return errorMessage;
        });
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    create() {
        var folderName = this.folderName();

        this.creationTaskStarted = true;
        this.creationTask.resolve(folderName.toLowerCase());
        dialog.close(this);
    }

    private folderExists(folderName: string): boolean {
        return this.foldersLowerCase.contains(folderName.toLowerCase());
    }
}

export = createFolderInFilesystem;
