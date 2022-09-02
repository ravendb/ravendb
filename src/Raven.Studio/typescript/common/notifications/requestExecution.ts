
class requestExecution {

    spinnerVisible = false;
    alertVisible = false;
    completed = false;

    private spinnerTimeout: number;
    private alertTimeout: number;

    constructor(private timeForSpinner: number, private timeToAlert: number = 0, private sync: () => void) {
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

export = requestExecution;
