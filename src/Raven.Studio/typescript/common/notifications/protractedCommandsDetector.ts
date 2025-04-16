import requestExecution = require("common/notifications/requestExecution");

class protractedCommandsDetector {
    static instance = new protractedCommandsDetector();

    private requestsInProgress: requestExecution[] = [];

    showSpinner = ko.observable<boolean>(false);
    showServerNotResponding = ko.observable<boolean>(false);

    constructor() {
        this.showSpinner.subscribe((show: boolean) => {
            if (show) {
                $(".protracted-request-message").removeClass("hidden");
            } else {
                $(".protracted-request-message").addClass("hidden");
            }
        });
    }

    requestStarted(timeForSpinner: number, timeForAlert = 0): requestExecution {
        const execution = new requestExecution(timeForSpinner, timeForAlert, () => this.sync());

        this.requestsInProgress.push(execution);

        return execution;
    }

    clearRequests() {
        this.requestsInProgress.forEach(x => x.markCompleted());
        this.requestsInProgress = [];
    }

    private sync() {
        this.showSpinner(this.requestsInProgress.some(x => x.spinnerVisible));

        this.requestsInProgress = this.requestsInProgress.filter(x => !x.completed);
    }
}

export = protractedCommandsDetector;
