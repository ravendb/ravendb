class changeSubscription {

    private executed = false;

    private readonly onOff: () => void;

    constructor(onOff: () => void) {
        this.onOff = onOff;
    }

    off() {
        if (!this.executed) {
            this.executed = true;
            this.onOff();
        }
    }
}

export = changeSubscription;
