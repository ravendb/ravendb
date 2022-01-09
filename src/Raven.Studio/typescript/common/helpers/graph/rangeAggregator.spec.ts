import rangeAggregator from "common/helpers/graph/rangeAggregator";

describe("rangeAggregator", function () {
    it('Calculate number of concurrent working indexes - When the work start time exactly equals the work end time', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 2 },
            { startTime: 15, endTime: 15 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(3);
        expect(item.numberOfItems).toEqual(0);
        item = output[2];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(16);
        expect(item.numberOfItems).toEqual(0);

       expect(finder.maxConcurrentItems).toEqual(1);
    });

    it('Calculate number of concurrent working indexes - When one range starts immidiately after previous range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 14 },
            { startTime: 15, endTime: 36 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(1);
        item = output[2];
        expect(item.pointInTime).toEqual(37);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(1);
    });

    it('Calculate number of concurrent working indexes - More than one index is working at the same time ranges', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 2, endTime: 6 },
            { startTime: 8, endTime: 10 },
            { startTime: 8, endTime: 10 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(2);
        item = output[1];
        expect(item.pointInTime).toEqual(7);
        expect(item.numberOfItems).toEqual(0);
        item = output[2];
        expect(item.pointInTime).toEqual(8);
        expect(item.numberOfItems).toEqual(2);
        item = output[3];
        expect(item.pointInTime).toEqual(11);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(2);
    });

    it('Calculate number of concurrent working indexes - Multiple time ranges', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 20, endTime: 65 },
            { startTime: 25, endTime: 60 },
            { startTime: 90, endTime: 96 },
            { startTime: 200, endTime: 233 },
            { startTime: 0, endTime: 1 },
            { startTime: 97, endTime: 117 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(0);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(0);
        item = output[2];
        expect(item.pointInTime).toEqual(20);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(25);
        expect(item.numberOfItems).toEqual(2);
        item = output[4];
        expect(item.pointInTime).toEqual(61);
        expect(item.numberOfItems).toEqual(1);
        item = output[5];
        expect(item.pointInTime).toEqual(66);
        expect(item.numberOfItems).toEqual(0);
        item = output[6];
        expect(item.pointInTime).toEqual(90);
        expect(item.numberOfItems).toEqual(1);
        item = output[7];
        expect(item.pointInTime).toEqual(97);
        expect(item.numberOfItems).toEqual(1);
        item = output[8];
        expect(item.pointInTime).toEqual(118);
        expect(item.numberOfItems).toEqual(0);
        item = output[9];
        expect(item.pointInTime).toEqual(200);
        expect(item.numberOfItems).toEqual(1);
        item = output[10];
        expect(item.pointInTime).toEqual(234);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(2);
    });

    it('Calculate number of concurrent working indexes - Single time range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([{ startTime: 2, endTime: 6 }]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(7);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(1);
    });

    it('Calculate number of concurrent working indexes - Two different, separated time ranges', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 11, endTime: 17 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(7);
        expect(item.numberOfItems).toEqual(0);
        item = output[2];
        expect(item.pointInTime).toEqual(11);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(18);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(1);
    });

    it('Calculate number of concurrent working indexes - Many indexes with the same start time, different end times', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 5, endTime: 70 },
            { startTime: 5, endTime: 13 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(3);
        item = output[1];
        expect(item.pointInTime).toEqual(14);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(71);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(3);
    });

    it('Calculate number of concurrent working indexes - Multiple indexes, some with the same start & end time points', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 8, endTime: 70 },
            { startTime: 1, endTime: 70 },
            { startTime: 12, endTime: 71 },
            { startTime: 12, endTime: 21 },
            { startTime: 5, endTime: 14 },
            { startTime: 6, endTime: 13 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(1);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(3);
        item = output[2];
        expect(item.pointInTime).toEqual(6);
        expect(item.numberOfItems).toEqual(4);
        item = output[3];
        expect(item.pointInTime).toEqual(8);
        expect(item.numberOfItems).toEqual(5);
        item = output[4];
        expect(item.pointInTime).toEqual(12);
        expect(item.numberOfItems).toEqual(7);
        item = output[5];
        expect(item.pointInTime).toEqual(14);
        expect(item.numberOfItems).toEqual(6);
        item = output[6];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(4);
        item = output[7];
        expect(item.pointInTime).toEqual(22);
        expect(item.numberOfItems).toEqual(3);
        item = output[8];
        expect(item.pointInTime).toEqual(71);
        expect(item.numberOfItems).toEqual(1);
        item = output[9];
        expect(item.pointInTime).toEqual(72);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(7);
    });

    it('Calculate number of concurrent working indexes - Many indexes with the same end time, different start times', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 8, endTime: 70 },
            { startTime: 1, endTime: 70 },
            { startTime: 12, endTime: 70 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(1);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(8);
        expect(item.numberOfItems).toEqual(3);
        item = output[3];
        expect(item.pointInTime).toEqual(12);
        expect(item.numberOfItems).toEqual(4);
        item = output[4];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(3);
        item = output[5];
        expect(item.pointInTime).toEqual(71);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(4);
    });

    it('Calculate number of concurrent working indexes - Multiple indexes with overlaping time ranges - sample flow 1', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 20, endTime: 40 },
            { startTime: 2, endTime: 10 },
            { startTime: 3, endTime: 27 },
            { startTime: 0, endTime: 43 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(0);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(3);
        expect(item.numberOfItems).toEqual(3);
        item = output[3];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(4);
        item = output[4];
        expect(item.pointInTime).toEqual(11);
        expect(item.numberOfItems).toEqual(3);
        item = output[5];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(2);
        item = output[6];
        expect(item.pointInTime).toEqual(20);
        expect(item.numberOfItems).toEqual(3);
        item = output[7];
        expect(item.pointInTime).toEqual(28);
        expect(item.numberOfItems).toEqual(2);
        item = output[8];
        expect(item.pointInTime).toEqual(41);
        expect(item.numberOfItems).toEqual(1);
        item = output[9];
        expect(item.pointInTime).toEqual(44);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(4);
    });

    it('Calculate number of concurrent working indexes - Multiple indexes with overlaping time ranges - sample flow 2', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 2, endTime: 10 },
            { startTime: 1, endTime: 7 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(1);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(3);
        item = output[3];
        expect(item.pointInTime).toEqual(8);
        expect(item.numberOfItems).toEqual(2);
        item = output[4];
        expect(item.pointInTime).toEqual(11);
        expect(item.numberOfItems).toEqual(1);
        item = output[5];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(3);
    });

    it('Calculate number of concurrent working indexes - Multiple indexes with overlaping time ranges - sample flow 3', () => {
        
        let finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 2, endTime: 10 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(11);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(15);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(2);
    });

    it('Calculate number of concurrent working indexes - Index time range is within another index time range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 3, endTime: 4 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(3);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(5);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(7);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(2);
    });

    it('Calculate number of concurrent working indexes - Many indexes time range within another range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 600 },
            { startTime: 30, endTime: 40 },
            { startTime: 50, endTime: 60 },
            { startTime: 70, endTime: 80 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        expect(item.pointInTime).toEqual(2);
        expect(item.numberOfItems).toEqual(1);
        item = output[1];
        expect(item.pointInTime).toEqual(30);
        expect(item.numberOfItems).toEqual(2);
        item = output[2];
        expect(item.pointInTime).toEqual(41);
        expect(item.numberOfItems).toEqual(1);
        item = output[3];
        expect(item.pointInTime).toEqual(50);
        expect(item.numberOfItems).toEqual(2);
        item = output[4];
        expect(item.pointInTime).toEqual(61);
        expect(item.numberOfItems).toEqual(1);
        item = output[5];
        expect(item.pointInTime).toEqual(70);
        expect(item.numberOfItems).toEqual(2);
        item = output[6];
        expect(item.pointInTime).toEqual(81);
        expect(item.numberOfItems).toEqual(1);
        item = output[7];
        expect(item.pointInTime).toEqual(601);
        expect(item.numberOfItems).toEqual(0);

        expect(finder.maxConcurrentItems).toEqual(2);
    });
});
