
class requestExecution {

    spinnerVisible = false;
    alertVisible = false;
    completed = false;

    private spinnerTimeout: number;
    private alertTimeout: number;

    constructor(private timeForSpinner: number, private timeToAlert: number = 0, private sync: Function) {
        this.setTimeouts();
    }

    markCompleted() {
        this.cleanState();
        this.sync();
        this.completed = true;
    }

    markProgress() {
        this.cleanState();
        this.setTimeouts();
        this.sync();
    }

    private cleanState() {
        clearTimeout(this.spinnerTimeout);
        if (this.alertTimeout) {
            clearTimeout(this.alertTimeout);
        }
        this.spinnerVisible = false;
        this.alertVisible = false;
    }

    private setTimeouts() {
        this.spinnerTimeout = setTimeout(() => {
            this.spinnerVisible = true;
            this.sync();
        }, this.timeForSpinner);

        if (this.timeToAlert > 0) {
            this.alertTimeout = setTimeout(() => {
                this.alertVisible = true;
                this.sync();
            }, this.timeToAlert);
        }
    }
}

class protractedCommandsDetector {
    static instance = new protractedCommandsDetector();

    private requestsInProgress = [] as Array<requestExecution>;

    showSpinner = ko.observable<boolean>(false);
    showServerNotResponding = ko.observable<boolean>(false);

    constructor() {
        this.showSpinner.subscribe((show: boolean) => $("body").toggleClass("processing", show));
    }

    requestStarted(timeForSpinner: number, timeForAlert: number = 0): requestExecution {
        const execution = new requestExecution(timeForSpinner, timeForAlert, () => this.sync());

        this.requestsInProgress.push(execution);

        return execution;
    }

    private sync() {
        this.showSpinner(_.some(this.requestsInProgress, x => x.spinnerVisible));

        //TODO: handle block UI ?

        _.remove(this.requestsInProgress, x => x.completed);
    }

    private showServerNotRespondingAlert() {
        this.showServerNotResponding(true);
        //TODO: update with new style
        $.blockUI({ message: '<div id="longTimeoutMessage"><span> This is taking longer than usual</span><br/><span>(Waiting for server to respond)</span></div>' });
    }

}

export = protractedCommandsDetector;