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
        jwerty.key("enter", () => this.enterKeyPressed());
        $(dialogViewModelBase.dialogSelector).focus();
    }

    detached() {
        if (this.elementToFocusOnDismissal) {
            $(this.elementToFocusOnDismissal).focus();
        }

        $(dialogViewModelBase.dialogSelector).unbind('keydown.jwerty');
    }

    private enterKeyPressed() {
        var acceptButton = <HTMLAnchorElement>$(".modal-footer:visible .btn-primary")[0];
        if (acceptButton && acceptButton.click) {
            acceptButton.click();
        }
    }
}

export = dialogViewModelBase;