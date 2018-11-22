/// <reference path="../../../../typings/tsd.d.ts" />
class rangeAggregator {

    items: Array<workTimeUnit> = [];
    indexesWork: Array<indexesWorkData> = [];
    ptrStart: number;
    ptrEnd: number;
    maxConcurrentIndexes: number;

    constructor(inputItems: Array<[Date, Date]>) {
        this.maxConcurrentIndexes = 0;
        // Subtract 1 from endTime if we can
        // Because the endTime of one work batch can be reported by the server
        // as exactly the same time as the startTime of ANOTHER work batch for the SAME index

        this.items = inputItems.map((x) => (
            (x[0].getTime() === x[1].getTime()) ?
                { startTime: x[0].getTime(), endTime: x[1].getTime() } :
                { startTime: x[0].getTime(), endTime: x[1].getTime() -1 }
        ));
    }

    inputItems(inputItems: Array<workTimeUnit>) {
        this.items = inputItems;
    }

    aggregate() : Array<indexesWorkData> {
        // 1. Sort the times array by startTime & endTime value
        this.items.sort((a, b) => a.startTime === b.startTime ? a.endTime - b.endTime : a.startTime - b.startTime);

        // 2. Create the indexesWork Array
        if (this.items.length !== 0) {

            // 3. Push first item
            this.indexesWork.push({ pointInTime: this.items[0].startTime, numberOfIndexesWorking: 1 });
            this.indexesWork.push({ pointInTime: this.items[0].endTime + 1, numberOfIndexesWorking: 0 });
            this.maxConcurrentIndexes = 1;

            // 4. Push all other items 
            for (let i = 1; i < this.items.length; i++) {
                this.pushRange(this.items[i].startTime, this.items[i].endTime);
            }
        }
        return this.indexesWork;
    }

    pushRange(startTime: number, endTime: number) {

        // 1. Find appropriate start position for the new element
        
        this.ptrStart = _.sortedIndexBy(this.indexesWork, { pointInTime: startTime }, x => x.pointInTime);

        let previousValue = (this.ptrStart === 0) ? this.indexesWork[0].numberOfIndexesWorking : this.indexesWork[this.ptrStart - 1].numberOfIndexesWorking;
       
        if (this.ptrStart === this.indexesWork.length) {
            // 1.1 Push new item in the end
            let newValue = previousValue + 1;
            this.indexesWork.push({ pointInTime: startTime, numberOfIndexesWorking: newValue });
            if (newValue > this.maxConcurrentIndexes) { this.maxConcurrentIndexes++; };
        } else if (this.indexesWork[this.ptrStart].pointInTime === startTime) {
            // 1.2 Only increase counter
            let newValue = ++(this.indexesWork[this.ptrStart].numberOfIndexesWorking);
            if (newValue > this.maxConcurrentIndexes) { this.maxConcurrentIndexes++; };
        } else if (this.indexesWork[this.ptrStart].pointInTime !== startTime) {
            // 1.3 Create new element in the start/middle 
            let newValue = previousValue + 1;
            this.indexesWork.splice(this.ptrStart, 0, { pointInTime: startTime, numberOfIndexesWorking: newValue });
            if (newValue > this.maxConcurrentIndexes) { this.maxConcurrentIndexes++; };
        }

        // 2. Find appropriate end position for the new element AND update working indexes counter along the way...
        let i = this.ptrStart;
        while ((i < this.indexesWork.length) && (endTime + 1 > this.indexesWork[i].pointInTime)) {
            if ((i + 1 < this.indexesWork.length) && (endTime + 1 > this.indexesWork[i + 1].pointInTime)) {               
                let newValue = ++(this.indexesWork[i + 1].numberOfIndexesWorking);
                if (newValue > this.maxConcurrentIndexes) { this.maxConcurrentIndexes++; };
            }
            i++;
        }
        this.ptrEnd = i;

        previousValue = (this.ptrEnd === 0) ? this.indexesWork[0].numberOfIndexesWorking : this.indexesWork[this.ptrEnd - 1].numberOfIndexesWorking - 1 ;

        if (this.ptrEnd === this.indexesWork.length) {
            // 2.1 Push new item in the end
            this.indexesWork.push({ pointInTime: endTime + 1, numberOfIndexesWorking: 0 });
        } else if (this.indexesWork[this.ptrEnd].pointInTime !== endTime + 1) {
            // 2.2 Create new element in the middle
            this.indexesWork.splice(this.ptrEnd, 0, { pointInTime: endTime + 1, numberOfIndexesWorking: previousValue });
        }
    }
}

export = rangeAggregator;
