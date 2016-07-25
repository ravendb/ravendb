import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class resolveConflict extends dialogViewModelBase {

    resolveTask = $.Deferred<void>();
    resolveTaskStarted = false;

    message = ko.observable<string>('');
    title = ko.observable<string>('');

    constructor(message: string, title: string) {
        super();
        this.message(message);
        this.title(title);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.resolveTaskStarted) {
            this.resolveTask.reject();
        }
    }

    resolve() {
        this.resolveTaskStarted = true;
        this.resolveTask.resolve();
        dialog.close(this);
    }
}

export = resolveConflict; 
