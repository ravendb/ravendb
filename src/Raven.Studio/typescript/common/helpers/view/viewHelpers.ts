/// <reference path="../../../../typings/tsd.d.ts" />

import app = require("durandal/app");
import confirmationDialog = require("viewmodels/common/confirmationDialog");
import generalUtils = require("common/generalUtils");

class viewHelpers {

    static confirmationMessage(title: string, confirmationMessage: string, options?: confirmationDialogOptions): JQueryPromise<confirmDialogResult> {
        const viewTask = $.Deferred<confirmDialogResult>();
        options = Object.assign({
            defaultOption: null,
            forceRejectWithResolve: false,
            html: false,
            buttons: ["No", "Yes"]
        } as confirmationDialogOptions, options);
        
        if (!options.html) {
            confirmationMessage = generalUtils.escapeHtml(confirmationMessage);
        }

        app.showBootstrapDialog(new confirmationDialog(confirmationMessage, title, options.buttons))
            .done((answer) => {
                const isConfirmed = answer === _.last(options.buttons);
                if (isConfirmed) {
                    viewTask.resolve({ can: true });
                } else if (!options.forceRejectWithResolve) {
                    viewTask.reject();
                } else {
                    // answer is null when user 
                    if (answer != null) {
                        viewTask.resolve({ can: false });
                    } else {
                        viewTask.resolve({ can: _.last(options.buttons) === options.defaultOption });
                    }
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

    public static asyncValidationCompleted(context: KnockoutValidationGroup, callback?: Function) {
        const cb = (...args: any[]) => {
            return callback ? callback(...args) : undefined
        };

        const deferred = $.Deferred<void>();

        const validationGroup = (context as any)();
        const keys = _.keys(validationGroup);

        const asyncValidations = [] as Array<KnockoutObservable<boolean>>;

        keys.forEach(key => {
            if ("isValidating" in validationGroup[key]) {
                asyncValidations.push(validationGroup[key].isValidating);
            }
        });

        if (asyncValidations.length === 0 || _.every(asyncValidations, x => !x())) {
            cb();
            deferred.resolve();
            return deferred.promise();
        }

        // there are any validations in progress, await them

        let subscriptions = [] as Array<KnockoutSubscription>;

        const onUpdate = () => {
            if (_.every(asyncValidations, x => !x())) {
                // all validators completed its work, clean up and call callback
                subscriptions.forEach(x => x.dispose());
                cb();
                deferred.resolve();
            }
        }

        subscriptions = asyncValidations.map(v => v.subscribe(() => onUpdate()));

        return deferred.promise();
    }

    static getPageHostDimenensions(): [number, number] {
        const $pageHostRoot = $(".dynamic-container");

        return [$pageHostRoot.innerWidth(), $pageHostRoot.innerHeight()];
    }

    static animate(selector: JQuery, classToApply: string) {
        selector.addClass(classToApply);

        selector.one('animationend webkitAnimationEnd oanimationend MSAnimationEnd', () => {
            selector.removeClass(classToApply);
        });
    }
}

export = viewHelpers;
