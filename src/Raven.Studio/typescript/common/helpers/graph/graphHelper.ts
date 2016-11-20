/// <reference path="../../../../typings/tsd.d.ts" />


class graphHelper {
    static collapseTimeRanges(input: Array<[Date, Date]>): Array<[Date, Date]> {
        if (input.length === 0) {
            return [];
        }

        const stack = [] as Array<[Date, Date]>;

        input.sort((a, b) => d3.ascending(a[0].getTime(), b[0].getTime()));

        stack.push(input[0]);

        for (let i = 1; i < input.length; i++) {
            const top = stack.last();

            if (top[1].getTime() < input[i][0].getTime()) {
                stack.push(input[i]);
            } else if (top[1].getTime() < input[i][1].getTime()) {
                const merged: [Date, Date] = [top[0], input[i][1]];
                stack.pop();
                stack.push(merged);
            }
        }

        return stack;
    }

    static truncText(input: string, measuredWidth: number, availableWidth: number, minWidth = 5): string {
        if (availableWidth >= measuredWidth) {
            return input;
        }
        if (availableWidth < minWidth) {
            return null;
        }

        const approxCharactersToTake = Math.floor(availableWidth * input.length / measuredWidth);
        return input.substr(0, approxCharactersToTake);
    }

    static drawArrow(ctx: CanvasRenderingContext2D, x: number, y: number, rightArrow: boolean) {
        ctx.beginPath();
        if (rightArrow) {
            ctx.fillStyle = "#a8acac";
            ctx.moveTo(x, y);
            ctx.lineTo(x + 7, y + 4);
            ctx.lineTo(x, y + 8);
        } else {
            ctx.fillStyle = "#ff5500";
            ctx.moveTo(x, y + 1);
            ctx.lineTo(x + 4, y + 8);
            ctx.lineTo(x + 8, y + 1);
        }
        

        ctx.fill();
    }

    /**
     *  Returns helper function which translates milliseconds to pixels
     */
    static extentGenerator(scale: d3.time.Scale<number, number>): (millis: number) => number {
        const domain = scale.domain();
        const range = scale.range();

        if (range.length !== 2) {
            throw new Error("extractPixelsPerSecond only supports range with 2 elements");
        }

        const domainExtent = domain[1].getTime() - domain[0].getTime();
        const rangeExtent = range[1] - range[0];

        return (millis: number) => millis * rangeExtent / domainExtent;
    }

    static timeRangeFromSortedRanges(input: Array<[Date, Date]>): [Date, Date] {
        if (input.length === 0) {
            return null;
        }

        const minDate = input[0][0];
        const maxDate = input[input.length - 1][1];
        return [minDate, maxDate];
    }

    /**
     * Divide elements
     * Ex. For Total width = 100, eleemntWidth = 20, elements = 2
     * We have:
     * | 20px padding | 20px element | 20px padding | 20px element | 20px padding |
     * So elements width stays the same and padding is divided equally,
     * We return start X (which in 20 px in this case)
     * and offset - as width between objects start (40px)
     */
    static computeStartAndOffset(totalWidth: number, elements: number, elementWidth: number): { start: number; offset: number } {
        const offset = (totalWidth - elementWidth * elements) / (elements + 1) + elementWidth;
        const start = offset - elementWidth;

        return {
            offset: offset,
            start: start
        };
    }


    private static readonly arrowConfig = {
        halfWidth: 6,
        height: 8   
    }

    static drawBezierDiagonal(ctx: CanvasRenderingContext2D, source: [number, number], target: [number, number], withArrow = false) {
        ctx.beginPath();

        const m = (source[1] + target[1]) / 2;

        ctx.moveTo(source[0], source[1]);
        ctx.bezierCurveTo(source[0], m, target[0], m, target[0], target[1]);
        ctx.stroke();

        if (withArrow) {
            ctx.beginPath();
            ctx.moveTo(target[0] - graphHelper.arrowConfig.halfWidth, target[1] + graphHelper.arrowConfig.height);
            ctx.lineTo(target[0], target[1]);
            ctx.lineTo(target[0] + graphHelper.arrowConfig.halfWidth, target[1] + graphHelper.arrowConfig.height);
            ctx.stroke();
        }
    }


}

export = graphHelper;
