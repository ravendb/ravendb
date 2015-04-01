import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class showDataDialog extends dialogViewModelBase {

    constructor(private title: string, private inputData: string, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    canActivate(args: any): any {
         return true;
    }

    attached() {
        super.attached();
        this.selectText();
    }

    deactivate() {
        $("#inputData").unbind('keydown.jwerty');
    }

    selectText() {
        $("#inputData").select();
    }

    close() {
        dialog.close(this);
    }
}

export = showDataDialog; 