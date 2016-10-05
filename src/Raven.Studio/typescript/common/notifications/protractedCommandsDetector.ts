class protractedCommandsDetector {
    static instance = new protractedCommandsDetector();

    showSpinner = ko.observable<boolean>(false);
    showServerNotResponding = ko.observable<boolean>(false);

    private requestsInProgress = 0;
    private spinnerTimeout = 0;
    private alertTimeout = 0;
    private biggestTimeToAlert = 0;

    constructor() {
        this.showSpinner.subscribe((show: boolean) => $("body").toggleClass("processing", show));
    }

    progressReceived(timeToAlert: number) {
        clearTimeout(this.alertTimeout);
        this.alertTimeout = setTimeout(() => this.showServerNotRespondingAlert(), timeToAlert);
    }

    requestStarted(timeToAlert: number) {
        this.requestsInProgress++;

        var isBiggestTimeToAlertUpdated = timeToAlert > this.biggestTimeToAlert;
        if ((this.requestsInProgress === 0 && timeToAlert > 0) || isBiggestTimeToAlertUpdated) {
            this.biggestTimeToAlert = timeToAlert;
            clearTimeout(this.spinnerTimeout);
            this.spinnerTimeout = setTimeout(() => this.showSpin(timeToAlert, isBiggestTimeToAlertUpdated), 1000);
        }
    }

    requestCompleted() {
        this.requestsInProgress--;
        if (this.requestsInProgress === 0) {
            clearTimeout(this.spinnerTimeout);
            clearTimeout(this.alertTimeout);
            this.alertTimeout = 0;
            this.spinnerTimeout = 0;
            this.allRequestsCompleted();
        }
    }

    private showSpin(timeToAlert: number, isBiggestTimeToAlertUpdated: boolean) {
        this.showSpinner(true);
        if (this.alertTimeout === 0 || isBiggestTimeToAlertUpdated) {
            clearTimeout(this.alertTimeout);
            this.biggestTimeToAlert = timeToAlert;
            this.alertTimeout = setTimeout(() => this.showServerNotRespondingAlert(), timeToAlert);
        }
    }

    private showServerNotRespondingAlert() {
        this.showServerNotResponding(true);
        //TODO: update with new style
        $.blockUI({ message: '<div id="longTimeoutMessage"><span> This is taking longer than usual</span><br/><span>(Waiting for server to respond)</span></div>' });
    }

    private allRequestsCompleted() {
        this.showSpinner(false);
        this.showServerNotResponding(false); //TODO: bind unblockui somewhere $.unblockUI();
    }
}

export = protractedCommandsDetector;