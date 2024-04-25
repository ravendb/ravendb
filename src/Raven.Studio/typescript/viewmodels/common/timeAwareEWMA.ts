// This is modified version of EWMA (exponentially weighted moving average)

export class timeAwareEWMA {
    private prevTime: number;
    private prevEwma: number;

    private noServerValueInterval: NodeJS.Timeout;
    private msSinceLastServerValue: number = 0;

    private readonly halfLife: number;

    value = ko.observable<number>(null);

    constructor(halfLife: number) {
        this.halfLife = halfLife;
    }

    private tick(value: number, time: Date = new Date()) {
        if (this.prevEwma == null || this.prevTime == null) {
            this.prevEwma = value;
            this.prevTime = time.getTime();

            this.value(Math.floor(value));
            this.startNoServerValueInterval();
            return;
        }

        const timeAsNumber = time.getTime();
        const timeDecay = timeAsNumber - this.prevTime;

        const alpha = 1 - Math.exp(Math.log(0.5) * timeDecay / this.halfLife);
        const ewma = alpha * value + (1 - alpha) * this.prevEwma;

        this.prevEwma = ewma;
        this.prevTime = timeAsNumber;

        this.value(Math.floor(ewma));
    }

    private startNoServerValueInterval() {
        this.noServerValueInterval = setInterval(() => {
            this.msSinceLastServerValue += 100;

            if (this.value() === 0) {
                return;
            }

            // If the server doesn't send any value for 10 seconds, the value will be set to 0
            if (this.msSinceLastServerValue >= 10_000) {
                this.value(0);
                return;
            }

            // If the server doesn't send any value for 4 seconds, ewma will be calculated with 0 value
            if (this.msSinceLastServerValue >= 4_000 && this.msSinceLastServerValue % 1_000 === 0) {
                this.tick(0);
            }
        }, 100);
    }

    private clearData() {
        this.prevEwma = null;
        this.prevTime = null;
        this.msSinceLastServerValue = 0;

        if (this.noServerValueInterval) {
            clearInterval(this.noServerValueInterval);
        }
    }

    handleServerTick(value: number) {
        // If the server sends a value, it will be set to (value / secondsSinceLastValue)
        const secondsSinceLastValue = this.msSinceLastServerValue / 1_000;
        const calculatedValue = secondsSinceLastValue ? (value / secondsSinceLastValue) : value;

        this.clearData();
        this.tick(calculatedValue);
    }
    
    reset() {
        this.clearData();
        this.value(null);
    }
}
