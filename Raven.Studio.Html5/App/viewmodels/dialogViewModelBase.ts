import dialog = require("plugins/dialog");

/*
 * Base view model for view models used in dialogs.
 * Provides some default functionality:
 *    - Optionallay focuses an element when the dialog is closed.
 *    - Listens for ESC to close the dialog.
 */
class dialogViewModelBase {
    static dialogSelector = ".messageBox";

    constructor(private elementToFocusOnDismissal?: string) {
    }

    attached() {
        jwerty.key("escape", () => dialog.close(this), this, dialogViewModelBase.dialogSelector);
        $(dialogViewModelBase.dialogSelector).focus();
    }

    detached() {
        if (this.elementToFocusOnDismissal) {
            $(this.elementToFocusOnDismissal).focus();
        }

        $(dialogViewModelBase.dialogSelector).unbind('keydown.jwerty');
    }
}

export = dialogViewModelBase;