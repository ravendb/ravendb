class changeSubscription {

    private executed = false;

    constructor(private onOff: () => void) {

    }

    off() {
        if (!this.executed) {
            this.executed = true;
            this.onOff();
        }
    }
}

export = changeSubscription;