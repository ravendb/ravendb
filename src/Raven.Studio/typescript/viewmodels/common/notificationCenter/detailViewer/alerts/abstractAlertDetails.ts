import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import notificationCenter = require("common/notifications/notificationCenter");
import alert = require("common/notifications/models/alert");
import notificationCenterSettings = require("common/notifications/notificationCenterSettings");

abstract class abstractAlertDetails extends dialogViewModelBase {

    footerPartialView = require("views/common/notificationCenter/detailViewer/alerts/footerPartial.html");
    
    readonly postponeOptions = notificationCenterSettings.postponeOptions;

    protected readonly alert: alert;
    protected readonly dismissFunction: () => void;
    protected readonly postponeFunction: (timeInSeconds: number) => JQueryPromise<void>;

    spinners = {
        postpone: ko.observable<boolean>(false)
    };

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close", "dismiss", "postpone");
        this.alert = alert;
        this.dismissFunction = () => notificationCenter.dismiss(alert);
        this.postponeFunction = (time: number) => notificationCenter.postpone(alert, time);
    }

    dismiss() {
        this.dismissFunction();
        this.close();
    }

    postpone(time: number) {
        this.spinners.postpone(true);
        this.postponeFunction(time)
            .always(() => this.spinners.postpone(false))
            .done(() => this.close());
    }
}

export = abstractAlertDetails;
