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

        var submit = this.enabled();

        if (submit) {
            super.enterKeyPressed();
        }

        return submit;
    }

    enabled() {
        return this.checkRequired(true);
    }

    // check inputs 
    checkRequired(allRequired: boolean) {
        if (allRequired) {
            var result = true;

            this.inputs.forEach(input => {
                if (input() == null || !input().trim()) {
                    result = false;
                }
            });

            return result;
        } else { // at least one is required
            var result = false;

            this.inputs.forEach(input => {
                if (input() != null && input().trim()) {
                    result = true;
                }
            });

            return result;
        }
    }

   
}

export = searchDialogViewModel;