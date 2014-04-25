import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

/*
 * Base view model for search dialogs.
 * Extends from dialogViewModelBase
 * Provides some extra functionality:
 *    - Listens to Enter key and submits the form only if all the fields have been completed
 */
class searchDialogViewModel extends dialogViewModelBase {
    static dialogSelector = ".messageBox";

    constructor(inputs: KnockoutObservable<string>[]) {
        super();
        this.inputs = inputs;
    }

    inputs: KnockoutObservable<string>[];

    close() {
        dialog.close(this);
    }

    enterKeyPressed() {
        if (this.inputs == null)
            return false;

        var submit: boolean = true;
        for (var i = 0; i < this.inputs.length; i++) {
            submit = this.inputs[i]() != null && this.inputs[i]().trim() != "";
        }

        if (submit) {
            super.enterKeyPressed();
        }

        return submit;
    }
}

export = searchDialogViewModel;