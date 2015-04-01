import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class executePatchConfirm extends dialogViewModelBase {

    public viewTask = $.Deferred();
    private wasConfirmed: boolean = false;

    executePatch() {
        this.viewTask.resolve();
        this.wasConfirmed = true;
        dialog.close(this);
    }

    cancel() {
        this.viewTask.reject();
        this.wasConfirmed = false;
        dialog.close(this);
    }

    detached() {
        super.detached();

        if (!this.wasConfirmed) {
            this.viewTask.reject();
        }
    }
}

export = executePatchConfirm;