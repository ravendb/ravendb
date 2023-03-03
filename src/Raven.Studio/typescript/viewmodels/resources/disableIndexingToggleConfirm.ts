import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class disableIndexingToggleConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/resources/disableIndexingToggleConfirm.html");

    text: string;
    confirmText: string;

    private readonly disable: boolean;

    constructor(disable: boolean) {
        super(null);
        this.disable = disable;

        this.text = disable ? "You're disabling" : "You're enabling";
        this.confirmText = disable ? "Disable indexing" : "Enable indexing";
    }
}

export = disableIndexingToggleConfirm;
