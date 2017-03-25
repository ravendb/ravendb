/// <reference path="../../../../typings/tsd.d.ts" />

import app = require("durandal/app");
import confirmationDialog = require("viewmodels/common/confirmationDialog");

class viewHelpers {
    static confirmationMessage(title: string, confirmationMessage: string, options: string[] = ["No", "Yes"], forceRejectWithResolve: boolean = false): JQueryPromise<confirmDialogResult> {
        const viewTask = $.Deferred<confirmDialogResult>();

        app.showBootstrapDialog(new confirmationDialog(confirmationMessage, title, options))
            .done((answer) => {
                var isConfirmed = answer === _.last(options);
                if (isConfirmed) {
                    viewTask.resolve({ can: true });
                } else if (!forceRejectWithResolve) {
                    viewTask.reject();
                } else {
                    viewTask.resolve({ can: false });
                }
            });

        return viewTask;
    }

    static isValid(context: KnockoutValidationGroup, showErrors = true): boolean {
        if (context.isValid()) {
            return true;
        } else {
            if (showErrors) {
                context.errors.showAllMessages();
            }
            return false;
        }
    }
}

export = viewHelpers;
