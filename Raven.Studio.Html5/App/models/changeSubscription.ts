class changeSubscription {
    constructor(private onOff: () => void) {

    }

    off() {
        this.onOff();
    }
}

export = changeSubscription;