/// <reference path="../../typings/tsd.d.ts"/>

import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class confirmViewModelBase extends dialogViewModelBase {

    private alreadyResolved = false;
    result = $.Deferred<confirmDialogResult>(); 

    confirm() {
        this.result.resolve({ can: true });
        this.alreadyResolved = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args: any) {
        if (!this.alreadyResolved) {
            this.result.resolve({ can: false });
        }
    }
}

export = confirmViewModelBase;
