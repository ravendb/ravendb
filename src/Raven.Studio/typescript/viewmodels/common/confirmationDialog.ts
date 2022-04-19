import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

/*
 * Generic confirmation dialog. We created this because Durandal's built-in 
 * message box doesn't handle keyboard shortcuts like ESC and Enter.
 */
class confirmationDialog extends dialogViewModelBase {

    view = require("views/common/confirmationDialog.html");

    private confirmationMessageAsHtml: string;

    private title: string;

    private buttonOptions: string[];

    private wideDialog: boolean;

    constructor(confirmationMessageAsHtml: string, title: string, buttonOptions: string[], wideDialog: boolean) {
        super();
        this.wideDialog = wideDialog;
        this.buttonOptions = buttonOptions;
        this.title = title;
        this.confirmationMessageAsHtml = confirmationMessageAsHtml;
    }

    onOptionClicked(option: string) {
        dialog.close(this, option);
    }

    close() {
        dialog.close(this, null);
    }
}

export = confirmationDialog;
