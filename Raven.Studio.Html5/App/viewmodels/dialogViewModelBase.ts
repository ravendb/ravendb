import dialog = require("plugins/dialog");

/*
 * Base view model for view models used in dialogs.
 * Provides some default functionality:
 *    - Optionallay focuses an element when the dialog is closed.
 *    - Listens for ESC to close the dialog.
 */
class dialogViewModelBase {
    static dialogSelector = ".messageBox";
    dialogSelectorName = "";

    constructor(private elementToFocusOnDismissal?: string) {
    }

    attached() {
        var that = this;
        jwerty.key("esc", e => {
            e.preventDefault();
            dialog.close(that);
        }, this, this.dialogSelectorName == "" ? dialogViewModelBase.dialogSelector : this.dialogSelectorName );
        jwerty.key("enter", () => this.enterKeyPressed(), this, dialogViewModelBase.dialogSelector);
        $(dialogViewModelBase.dialogSelector).focus();
    }

    deactivate(args) {
        $(this.dialogSelectorName == "" ? dialogViewModelBase.dialogSelector : this.dialogSelectorName).unbind('keydown.jwerty');
    }

    detached() {
        if (this.elementToFocusOnDismissal) {
            $(this.elementToFocusOnDismissal).focus();
        }
    }

    enterKeyPressed(): boolean {
        var acceptButton = <HTMLAnchorElement>$(".modal-footer:visible .btn-primary")[0];
        if (acceptButton && acceptButton.click) {
            acceptButton.click();
        }
        return true;
    }
}

export = dialogViewModelBase;