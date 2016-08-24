import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class showDataDialog extends dialogViewModelBase {

    constructor(private title: string, private inputData: string, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    attached() {
        super.attached();
        this.registerResizing("documentsResize");
        this.selectText();

        //TODO: create createKeyboardShortcut in dialog view model base and use it
        jwerty.key("CTRL+C, enter", e => {
            e.preventDefault();
            this.close();
        }, this, "#documentsText");

    }

    deactivate() {
        $("#inputData").unbind('keydown.jwerty');
    }

    detached() {
        super.detached();
        this.unregisterResizing("documentsResize");
    }

    selectText() {
        $("#inputData").select();
    }

    close() { //TODO: move to base class?
        dialog.close(this);
    }
}

export = showDataDialog; 
