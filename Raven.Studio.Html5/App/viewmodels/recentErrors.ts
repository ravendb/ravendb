import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import alertArgs = require("common/alertArgs");

class recentErrors extends dialogViewModelBase {

    resizerSelector = ".recentErrorsResizer";
    width = ko.observable<number>(600);
    height = ko.observable<number>(500);

    left: KnockoutComputed<number>;
    top: KnockoutComputed<number>;
    bodyHeight: KnockoutComputed<number>;
    
    constructor(private errors: KnockoutObservableArray<alertArgs>) {
        super();

        this.left = ko.computed<number>(() => -this.width() * 0.5);
        this.top = ko.computed<number>(() => -this.height() * 0.5);
        this.bodyHeight = ko.computed<number>(() => this.height() - 150);
    }

    attached() {
        // Expand the first error.
        if (this.errors().length > 0) {
            $("#errorDetailsCollapse0").collapse("show");
        }

        this.registerResizing();
    }

    detached() {
        super.detached();
        this.unregisterResizing();
    }

    clear() {
        this.errors.removeAll();
    }

    close() {
        dialog.close(this);
    }

    getErrorDetails(alert: alertArgs) {
        var error = alert.errorInfo;
        if (error != null && error.stackTrace) {
            return error.stackTrace.replace("\r\n", "\n");
        }

        return alert.details;
    }

    registerResizing() {
        var w = 0;
        var h = 0;
        var startX = 0;
        var startY = 0;
        var resizing = false;
        $(document).on("mousedown.recentErrorsResize", this.resizerSelector, (e: any) => {
            w = this.width();
            h = this.height();
            startX = e.pageX;
            startY = e.pageY;
            resizing = true;
        });

        $(document).on("mouseup.recentErrorsResize", "", (e: any) => {
            resizing = false;
        });

        $(document).on("mousemove.recentErrorsResize", "", (e: any) => {
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

    unregisterResizing() {
        $(document).off("mousedown.recentErrorsResize");
        $(document).off("mouseup.recentErrorsResize");
        $(document).off("mousemove.recentErrorsResize");
    }
}

export = recentErrors; 