import requestExecution = require("common/notifications/requestExecution");

class protractedCommandsDetector {
    static instance = new protractedCommandsDetector();

    private requestsInProgress: requestExecution[] = [];

    showSpinner = ko.observable<boolean>(false);
    showServerNotResponding = ko.observable<boolean>(false);

    constructor() {
        this.showSpinner.subscribe((show: boolean) => $("body").toggleClass("processing", show));
    }

    requestStarted(timeForSpinner: number, timeForAlert = 0): requestExecution {
        const execution = new requestExecution(timeForSpinner, timeForAlert, () => this.sync());

        this.requestsInProgress.push(execution);

        return execution;
    }

    private sync() {
        this.showSpinner(this.requestsInProgress.some(x => x.spinnerVisible));

        this.requestsInProgress = this.requestsInProgress.filter(x => x.completed);
    }

    private showServerNotRespondingAlert() {
        this.showServerNotResponding(true);
        $.blockUI({ message: '<div id="longTimeoutMessage"><span> This is taking longer than usual</span><br/><span>(Waiting for server to respond)</span></div>' });
    }

}

export = protractedCommandsDetector;
