/// <reference path="../../typings/tsd.d.ts" />

import moment = require("moment");

class timeHelpers {

    // don't make this method public, instead create field for given time period
    private static createTimeBasedObservable(updatePeriodInMillis: number) {
        const now: KnockoutObservable<moment.Moment> = ko.observable<moment.Moment>();
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

    static readonly utcNowWithMinutePrecision = timeHelpers.createTimeBasedObservable(60 * 1000);
    static readonly utcNowWithSecondPrecision = timeHelpers.createTimeBasedObservable(1000);
}
export = timeHelpers;
