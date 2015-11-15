import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");

class showDataDialog extends dialogViewModelBase {

    constructor(private title: string, private inputData: string, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    canActivate(args: any): any {
         return true;
    }

    attached() {
        super.attached();
        this.registerResizing("documentsResize");
        this.selectText();
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

    close() {
        dialog.close(this);
    }
}

export = showDataDialog; 
