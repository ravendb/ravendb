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

    static timeRangeFromSortedRanges(input: Array<[Date, Date]>): [Date, Date] {
        if (input.length === 0) {
            return null;
        }

        const minDate = input[0][0];
        const maxDate = input[input.length - 1][1];
        return [minDate, maxDate];
    }

}

export = graphHelper;
