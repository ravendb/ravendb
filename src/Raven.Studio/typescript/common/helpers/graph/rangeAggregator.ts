/// <reference path="../../../../typings/tsd.d.ts" />
class rangeAggregator {

    items: Array<workTimeUnit> = [];
    workData: Array<workData> = [];
    ptrStart: number;
    ptrEnd: number;
    maxConcurrentItems: number;

    constructor(inputItems: Array<[Date, Date]>) {
        this.maxConcurrentItems = 0;
        // Subtract 1 from endTime if we can
        // Because the endTime of one work batch can be reported by the server
        // as exactly the same time as the startTime of ANOTHER work batch for the SAME item

        this.items = inputItems.map((x) => (
            (x[0].getTime() === x[1].getTime()) ?
                { startTime: x[0].getTime(), endTime: x[1].getTime() } :
                { startTime: x[0].getTime(), endTime: x[1].getTime() -1 }
        ));
    }

    inputItems(inputItems: Array<workTimeUnit>) {
        this.items = inputItems;
    }

    aggregate() : Array<workData> {
        // 1. Sort the times array by startTime & endTime value
        this.items.sort((a, b) => a.startTime === b.startTime ? a.endTime - b.endTime : a.startTime - b.startTime);

        // 2. Create the workData Array
        if (this.items.length !== 0) {

            // 3. Push first item
            this.workData.push({ pointInTime: this.items[0].startTime, numberOfItems: 1 });
            this.workData.push({ pointInTime: this.items[0].endTime + 1, numberOfItems: 0 });
            this.maxConcurrentItems = 1;

            // 4. Push all other items 
            for (let i = 1; i < this.items.length; i++) {
                this.pushRange(this.items[i].startTime, this.items[i].endTime);
            }
        }
        return this.workData;
    }

    pushRange(startTime: number, endTime: number) {

        // 1. Find appropriate start position for the new element
        
        this.ptrStart = _.sortedIndexBy(this.workData, { pointInTime: startTime }, x => x.pointInTime);

        let previousValue = (this.ptrStart === 0) ? this.workData[0].numberOfItems : this.workData[this.ptrStart - 1].numberOfItems;
       
        if (this.ptrStart === this.workData.length) {
            // 1.1 Push new item in the end
            let newValue = previousValue + 1;
            this.workData.push({ pointInTime: startTime, numberOfItems: newValue });
            if (newValue > this.maxConcurrentItems) { this.maxConcurrentItems++; };
        } else if (this.workData[this.ptrStart].pointInTime === startTime) {
            // 1.2 Only increase counter
            let newValue = ++(this.workData[this.ptrStart].numberOfItems);
            if (newValue > this.maxConcurrentItems) { this.maxConcurrentItems++; };
        } else if (this.workData[this.ptrStart].pointInTime !== startTime) {
            // 1.3 Create new element in the start/middle 
            let newValue = previousValue + 1;
            this.workData.splice(this.ptrStart, 0, { pointInTime: startTime, numberOfItems: newValue });
            if (newValue > this.maxConcurrentItems) { this.maxConcurrentItems++; };
        }

        // 2. Find appropriate end position for the new element AND update working items counter along the way...
        let i = this.ptrStart;
        while ((i < this.workData.length) && (endTime + 1 > this.workData[i].pointInTime)) {
            if ((i + 1 < this.workData.length) && (endTime + 1 > this.workData[i + 1].pointInTime)) {               
                let newValue = ++(this.workData[i + 1].numberOfItems);
                if (newValue > this.maxConcurrentItems) { this.maxConcurrentItems++; };
            }
            i++;
        }
        this.ptrEnd = i;

        previousValue = (this.ptrEnd === 0) ? this.workData[0].numberOfItems : this.workData[this.ptrEnd - 1].numberOfItems - 1 ;

        if (this.ptrEnd === this.workData.length) {
            // 2.1 Push new item in the end
            this.workData.push({ pointInTime: endTime + 1, numberOfItems: 0 });
        } else if (this.workData[this.ptrEnd].pointInTime !== endTime + 1) {
            // 2.2 Create new element in the middle
            this.workData.splice(this.ptrEnd, 0, { pointInTime: endTime + 1, numberOfItems: previousValue });
        }
    }
}

export = rangeAggregator;
