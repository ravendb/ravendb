/// <reference path="../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import dialog = require("plugins/dialog");
import { jwerty } from "jwerty";

type dialogViewModelBaseOptions = {
    elementToFocusOnDismissal?: string;
    dialogSelectorName?: string;
}

abstract class dialogViewModelBase {

    abstract view: { default: string };
    
    getView() {
        if (!this.view) {
            throw new Error("Looks like you forgot to define view in: " + this);
        }
        if (!this.view.default.trim().startsWith("<")) {
            console.warn("View doesn't start with '<'");
        }
        return this.view.default;
    }
    
    protected activeDatabase = activeDatabaseTracker.default.database;
    static readonly dialogSelector = ".modal-dialog";

    private readonly elementToFocusOnDismissal: string;
    private readonly dialogSelector: string;
    private disposableActions: Array<disposable> = [];

    pluralize = pluralizeHelpers.pluralize;

    constructor(options?: dialogViewModelBaseOptions) {
        if (options) {
            this.elementToFocusOnDismissal = options.elementToFocusOnDismissal;
            this.dialogSelector = options.dialogSelectorName || dialogViewModelBase.dialogSelector;
        } else {
            this.dialogSelector = dialogViewModelBase.dialogSelector;
        }
    }

    protected bindToCurrentInstance(...methods: Array<keyof this & string>) {
        _.bindAll(this, ...methods);
    }

    attached() {
        jwerty.key("enter", () => this.enterKeyPressed());
    }

    deactivate(args: any) {
        $(document).unbind('keydown.jwerty');
        
        this.disposableActions.forEach(f => f.dispose());
        this.disposableActions = [];
    }

    detached() {
        if (this.elementToFocusOnDismissal) {
            $(this.elementToFocusOnDismissal).focus();
        }
    }

    compositionComplete(view?: any, parent?: any) {
        setTimeout(() => this.setInitialFocus(), 100); // We have to time-delay this, else it never receives focus.
    }

    close() {
        dialog.close(this);
    }

    protected registerDisposable(disposable: disposable) {
        this.disposableActions.push(disposable);
    }

    protected setInitialFocus() {
        const autoFocusElement = $(this.dialogSelector + " [autofocus]");
        if (autoFocusElement.length) {
            autoFocusElement.focus();
            autoFocusElement.select();
        } else {
            $(this.dialogSelector).focus();
        }
    }

    protected enterKeyPressed(): boolean {
        const confirmButton = this.getConfirmButton();
        if (confirmButton && confirmButton.click) {
            confirmButton.click();
        }

        return true;
    }

    protected getConfirmButton(): HTMLElement {
        return $(".modal-footer:visible .btn-primary")[0] as HTMLElement;
    }

    protected isValid(context: KnockoutValidationGroup, showErrors = true): boolean {
        if (context.isValid()) {
            return true;
        } else {
            if (showErrors) {
                context.errors.showAllMessages();
            }
            return false;
        }
    }

    protected setupDisableReasons(container: string) {
        $('.has-disable-reason').tooltip({
            container: container
        });
    }

}

export = dialogViewModelBase;
