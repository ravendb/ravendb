/// <reference path="../../typings/tsd.d.ts"/>

import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

abstract class confirmViewModelBase<T extends confirmDialogResult> extends dialogViewModelBase {

    private alreadyResolved = false;
    result = $.Deferred<T>(); 

    confirm() {
        this.result.resolve(this.prepareResponse(true));
        this.alreadyResolved = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args: any) {
        super.deactivate(args);
        
        if (!this.alreadyResolved) {
            this.result.resolve(this.prepareResponse(false));
        }
    }

    protected prepareResponse(can: boolean): T {
        return {
            can: can
        } as T;
    }

}

export = confirmViewModelBase;
