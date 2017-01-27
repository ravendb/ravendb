/// <reference path="../../typings/tsd.d.ts" />

class timeHelpers {

    static readonly utcNowWithMinutePrecision = timeHelpers.createTimeBasedObservable(60 * 1000);

    // don't make this method public, instead create field for given time period
    private static createTimeBasedObservable(updatePeriodInMillis: number) {
        let now: KnockoutObservable<moment.Moment> = ko.observable<moment.Moment>();
        let interval: number;

        const computed = ko.pureComputed(() => now());

        computed.subscribe(() => {
            now(moment.utc());
            interval = setInterval(() => now(moment.utc()), updatePeriodInMillis);
        }, this, "awake");

        computed.subscribe(() => {
            clearInterval(interval);
            interval = undefined;
        }, this, "asleep");

        return computed;
    }
}
export = timeHelpers;
