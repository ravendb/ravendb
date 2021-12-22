import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import notificationCenter = require("common/notifications/notificationCenter");
import performanceHint = require("common/notifications/models/performanceHint");

abstract class abstractPerformanceHintDetails extends dialogViewModelBase {

    footerPartialView = require("views/common/notificationCenter/detailViewer/performanceHint/footerPartial.html");
    
    protected readonly hint: performanceHint;
    protected readonly dismissFunction: () => void;

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close", "dismiss");
        this.hint = hint;
        this.dismissFunction = () => notificationCenter.dismiss(hint);
    }

    dismiss() {
        this.dismissFunction();
        this.close();
    }
}

export = abstractPerformanceHintDetails;
