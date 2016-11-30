/// <reference path="../../../../typings/tsd.d.ts" />

class gapFinder {

    domain: Date[] = [];
    gapsPositions: timeGapInfo[] = [];

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

            const lastEnd = this.domain.last();
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
        const totalTime = gapFinder.calculateTotalTime(domain);

        const gapsCount = domain.length / 2 - 1;
        const widthExceptGrapsPadding = totalWidth - paddingBetweenGaps * gapsCount;

        const extentFunc = (period: number) => period * widthExceptGrapsPadding / totalTime;

        let currentX = 0;

        const range = [] as Array<number>;

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
        const result = [] as Array<Date>;

        for (let i = 0; i < domain.length / 2; i++) {
            const s = domain[2 * i];
            const e = domain[2 * i + 1];

            if (e.getTime() < start.getTime()) {
                continue;
            }

            if (s.getTime() > end.getTime()) {
                continue;
            }

            if (s.getTime() < start.getTime() && e.getTime() > end.getTime()) {
                result.push(start);
                result.push(end);
                continue;
            }

            if (s.getTime() < start.getTime()) {
                result.push(start);
                result.push(e);
                continue;
            }

            if (e.getTime() > end.getTime()) {
                result.push(s);
                result.push(end);
                continue;
            }

            result.push(s);
            result.push(e);
        }

        return result;
    }
    
}

export = gapFinder;