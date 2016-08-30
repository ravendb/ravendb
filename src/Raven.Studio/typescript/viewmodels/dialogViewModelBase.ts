/// <reference path="../../typings/tsd.d.ts"/>

import dialog = require("plugins/dialog");

/*
 * Base view model for view models used in dialogs.
 * Provides some default functionality:
 *    - Optionallay focuses an element when the dialog is closed.
 *    - Listens for ESC to close the dialog.
 */
class dialogViewModelBase {
    static dialogSelector = ".messageBox";
    dialogSelectorName = "";
    width = ko.observable<number>(500);
    height = ko.observable<number>(500);
    left: KnockoutComputed<number>;
    top: KnockoutComputed<number>;
    bodyHeight: KnockoutComputed<number>;
    isFocused = ko.observable(false);

    constructor(private elementToFocusOnDismissal?: string) {
        this.left = ko.computed<number>(() => -this.width() * 0.5);
        this.top = ko.computed<number>(() => -this.height() * 0.5);
        this.bodyHeight = ko.computed<number>(() => this.height() - 150);
    }

    attached() {
        jwerty.key("esc", e => this.escapeKeyPressed(e), this, this.dialogSelectorName === "" ? dialogViewModelBase.dialogSelector : this.dialogSelectorName);
        jwerty.key("enter", () => this.enterKeyPressed(), this, dialogViewModelBase.dialogSelector);
    }

    deactivate(args: any) {
        $(this.dialogSelectorName === "" ? dialogViewModelBase.dialogSelector : this.dialogSelectorName).unbind("keydown.jwerty");
    }

    detached() {
        if (this.elementToFocusOnDismissal) {
            $(this.elementToFocusOnDismissal).focus();
        }
    }

    compositionComplete(view: any, parent: any) {
        setTimeout(() => this.setInitialFocus(), 100); // We have to time-delay this, else it never receives focus.
    }

    setInitialFocus() {
        var autoFocusElement = $(".messageBox [autofocus]");
        if (autoFocusElement.length) {
            autoFocusElement.focus();
            autoFocusElement.select();
        } else {
            $(dialogViewModelBase.dialogSelector).focus();
        }
    }

    enterKeyPressed(): boolean {
        var acceptButton = <HTMLAnchorElement>$(".modal-footer:visible .btn-primary")[0];
        if (acceptButton && acceptButton.click) {
            acceptButton.click();
        }

        return true;
    }

    escapeKeyPressed(e: KeyboardEvent) {
        e.preventDefault();
        dialog.close(this);
    }

    unregisterResizing(id:string) {
        $(document).off("mousedown." + id);
        $(document).off("mouseup." + id);
        $(document).off("mousemove." + id);
    }

    registerResizing(id: string, resizerSelector = ".dialogResizer") {
        var w = 0;
        var h = 0;
        var startX = 0;
        var startY = 0;
        var resizing = false;
        $(document).on("mousedown." + id, resizerSelector, (e: any) => {
            w = this.width();
            h = this.height();
            startX = e.pageX;
            startY = e.pageY;
            resizing = true;
        });

        $(document).on("mouseup." + id, "", (e: any) => {
            resizing = false;
        });

        $(document).on("mousemove." + id, "", (e: any) => {
            if (resizing) {
                var targetWidth = w + 2*(e.pageX - startX);
                var targetHeight = h + 2*(e.pageY - startY);

                if (targetWidth < 600) targetWidth = 600;
                if (targetHeight < 500) targetHeight = 500;

                this.width(targetWidth);
                this.height(targetHeight);

                if (e.stopPropagation) e.stopPropagation();
                if (e.preventDefault) e.preventDefault();
                e.cancelBubble = true;
                e.returnValue = false;
                return false;
            }
        });
    }

    protected alignBoxVertically() {
        var messageBoxHeight = parseInt($(".messageBox").css('height'), 10);
        
        // find element to alter margin-top - it should be outer html element in dialog view
        // we can find this by looking for element with data-view inside modalHost container. 
        $(".modalHost [data-view]").css('margin-top', Math.floor(-1 * messageBoxHeight / 2) + 'px');
    }
}

export = dialogViewModelBase;
