/// <reference path="../../../../typings/tsd.d.ts" />

import d3 = require("d3");

class gapFinder {

    domain: Date[] = []; // Activity Start & End times, i.e [start1, end1, start2, end2, etc...]
    gapsPositions: timeGapInfo[] = []; // Gaps Start Time + Duration 

    minTime: Date;
    maxTime: Date;

    constructor(dateRanges: Array<[Date, Date]>, private minGapTime: number) {
        this.sortDateRanges(dateRanges);
        this.computeGaps(dateRanges, minGapTime);
    }

    createScale(totalWidth: number, paddingBetweenGaps: number) {
        return gapFinder.createScaleInternal(totalWidth, paddingBetweenGaps, this.domain);
    }

    trimmedScale(timeRange: [Date, Date], totalWidth: number, paddingBetweenGaps: number): d3.time.Scale<number, number> {
        const trimmedDomain = gapFinder.trimDomain(this.domain, timeRange[0], timeRange[1]);

        return gapFinder.createScaleInternal(totalWidth, paddingBetweenGaps, trimmedDomain);
    }

    getGapInfoByTime(gapStartTime: Date): timeGapInfo {
        return (this.gapsPositions.find(g => g.start.getTime() === gapStartTime.getTime()));
    }

    /**
    *  Returns helper function which translates milliseconds to pixels in scale with gaps
    */
    static extentGeneratorForScaleWithGaps(scale: d3.time.Scale<number, number>): (millis: number) => number {
        const totalTime = gapFinder.calculateTotalTime(scale.domain());
        const totalRangeWidth = gapFinder.calculateRangeWidthExceptGaps(scale.range());

        return (millis: number) => millis * totalRangeWidth / totalTime;
    }

    private sortDateRanges(dateRanges: Array<[Date, Date]>) {
        dateRanges.sort((a, b) => d3.ascending(a[0].getTime(), b[0].getTime()));
    }

    private computeGaps(dateRanges: Array<[Date, Date]>, minGapTime: number) {
        if (dateRanges.length > 0) {
            let s = dateRanges[0][0];
            let e = dateRanges[0][1];
            this.minTime = s;
            this.maxTime = e;
            for (let i = 1; i < dateRanges.length; i++) {
                const newRange = dateRanges[i];

                if (this.minTime > newRange[0]) {
                    this.minTime = newRange[0];
                }

                if (this.maxTime < newRange[1]) {
                    this.maxTime = newRange[1];
                }

                if (newRange[0].getTime() > e.getTime() + minGapTime) {
                    this.pushRegion([s, e]);
                    s = newRange[0];
                }
                if (newRange[1].getTime() > e.getTime()) {
                    e = newRange[1];
                }
            }
            this.pushRegion([s, e]);
        }
    }

    private pushRegion(region: [Date, Date]) {
        if (this.domain.length > 0) {
            // since domain is not empty push gap

            const lastEnd = _.last(this.domain);
            const gapStart = region[0];
            this.gapsPositions.push({
                start: lastEnd,
                durationInMillis: gapStart.getTime() - lastEnd.getTime()
            });
        }

        this.domain.push(region[0]);
        this.domain.push(new Date(region[1].getTime() + 1)); // add 1 extra millisecond to avoid issues with rounding time
    }

    private static calculateTotalTime(domain: Date[]) {
        let totalTime = 0;

        for (let i = 0; i < domain.length / 2; i++) {
            const s = domain[2 * i];
            const e = domain[2 * i + 1];
            const periodDuration = e.getTime() - s.getTime();

            totalTime += periodDuration;
        }

        return totalTime;
    }

    private static calculateRangeWidthExceptGaps(input: number[]) {
        let width = 0;

        for (let i = 0; i < input.length / 2; i++) {
            const s = input[2 * i];
            const e = input[2 * i + 1];
            const periodDuration = e - s;

            width += periodDuration;
        }

        return width;

    }

    private static createScaleInternal(totalWidth: number, paddingBetweenGaps: number, domain: Date[]) {
        let totalTime = gapFinder.calculateTotalTime(domain);

        const gapsCount = domain.length / 2 - 1;
        const widthExceptGrapsPadding = totalWidth - paddingBetweenGaps * gapsCount;

        // This is a patch so that we don't divide by 0... See issue 6770..
        if (totalTime === 0) {
            totalTime = 1;
        }
        const extentFunc = (period: number) => period * widthExceptGrapsPadding / totalTime;

        let currentX = 0;

        const range: number[] = [];

        for (let i = 0; i < domain.length / 2; i++) {
            const s = domain[2 * i];
            const e = domain[2 * i + 1];
            const periodDuration = e.getTime() - s.getTime();
            const scaleExtent = extentFunc(periodDuration);

            range.push(currentX);
            currentX += scaleExtent;
            range.push(currentX);

            currentX += paddingBetweenGaps;
        }

        return d3.time.scale<number>()
            .range(range)
            .domain(domain);
    }

    private static trimDomain(domain: Date[], start: Date, end: Date): Date[] {
        const result: Date[] = [];

        // requested: |------|
        // items:              |----|  |-----|
        // result:    |------|
        if (end.getTime() < domain[0].getTime()) {
            result.push(start);
            result.push(end);
            return result;
        }

        // requested:                |------|
        // items:    |----|  |-----|
        // result:                   |------|
        if (start.getTime() > _.last(domain).getTime()) {
            result.push(start);
            result.push(end);
            return result;
        }

        let canSnapToLeft = true;

        for (let i = 0; i < domain.length / 2; i++) {
            const s = domain[2 * i];
            const e = domain[2 * i + 1];

            // item ends before requested start 
            // skip it
            if (e.getTime() < start.getTime()) {
                continue;
            }

            // item starts after requested end
            // skip it
            if (s.getTime() > end.getTime()) {
                continue;
            }

            // requested range falls into item range
            // show only requested range
            // requested:      |----|
            // item:       |------------|
            // result:         |----|
            if (s.getTime() < start.getTime() && e.getTime() > end.getTime()) {
                result.push(start);
                result.push(end);
                continue;
            }

            let effectiveStart = s;
            let effectiveEnd = e;

            if (canSnapToLeft && start.getTime() < s.getTime()) {
                effectiveStart = start;
            } else if (start.getTime() > s.getTime()) {
                effectiveStart = start;
            }

            // if last item or next item starts after requested end
            const isLastMatchingItem = e.getTime() === _.last(domain).getTime() || end.getTime() < domain[2 * i + 2].getTime();

            if (end.getTime() < e.getTime()) {
                effectiveEnd = end;
            } else if (end.getTime() > e.getTime() && isLastMatchingItem) {
                effectiveEnd = end;
            }

            result.push(effectiveStart);
            result.push(effectiveEnd);

            canSnapToLeft = false;
        }

        return result;
    }
    
}

export = gapFinder;
