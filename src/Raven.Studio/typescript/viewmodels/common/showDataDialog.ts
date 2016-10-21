import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class showDataDialog extends dialogViewModelBase {

    constructor(private title: string, private inputData: string, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    attached() {
        super.attached();
        this.selectText();

        //TODO: create createKeyboardShortcut in dialog view model base and use it
        jwerty.key("CTRL+C, enter", e => {
            e.preventDefault();
            this.close();
        }, this, "#documentsText");

    }

    deactivate() {
        //TODO: call super?
        $("#inputData").unbind('keydown.jwerty');
    }

    selectText() {
        $("#inputData").select();
    }

    close() { //TODO: move to base class?
        dialog.close(this);
    }
}

export = showDataDialog; 
