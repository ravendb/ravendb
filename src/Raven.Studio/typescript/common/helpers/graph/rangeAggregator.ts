/// <reference path="../../../../typings/tsd.d.ts" />

import d3 = require("d3");

class rangeAggregator {
    
    aggregation = [] as Array<aggregatedRange>;

    pushRange(start: number, end: number) {
        if (this.aggregation.length === 0) {
            this.aggregation.push({
                start: start,
                end: end,
                value: 1
            });
            return;
        }

        // before all
        if (end < this.aggregation[0].start) {
            if (this.aggregation[0].end !== start) {
                this.aggregation.unshift({
                    start: this.aggregation[0].end,
                    end: start,
                    value: 0
                });
            }

            this.aggregation.unshift({
                start: start,
                end: end,
                value: 1
            });
            return;
        }

        // after all
        if (start > this.aggregation.last().end) {
            if (this.aggregation.last().end !== start) {
                this.aggregation.push({
                    start: this.aggregation.last().end,
                    end: start,
                    value: 0
                });
            }

            this.aggregation.push({
                start: start,
                end: end,
                value: 1
            });
            return;
        }

        let currentPosition = start;

        if (start < this.aggregation[0].start) {
            this.aggregation.unshift({
                start: start,
                end: this.aggregation[0].start,
                value: 1
            });
            currentPosition = this.aggregation[0].start;
        } else {
            // seek to item to divide

        }

        //TODO:

    }
    
}

export = rangeAggregator;