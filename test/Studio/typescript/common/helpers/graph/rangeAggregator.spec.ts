import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import rangeAggregator = require("src/Raven.Studio/typescript/common/helpers/graph/rangeAggregator");
import chai = require("chai");

const helperUnderTest = 'common/helpers/graph/rangeAggregator';

describe(helperUnderTest, () => {

    it('Calculate number of concurrent working indexes - When the work start time exactly equals the work end time', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 2 },
            { startTime: 15, endTime: 15 }
        ]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(3);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(16);
        chai.expect(item.numberOfItems).to.equal(0);

       chai.expect(finder.maxConcurrentItems).to.equal(1);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(37);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(1);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(7);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(8);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(11);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(2);
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
        chai.expect(item.pointInTime).to.equal(0);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(20);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(25);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(61);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(66);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[6];
        chai.expect(item.pointInTime).to.equal(90);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[7];
        chai.expect(item.pointInTime).to.equal(97);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[8];
        chai.expect(item.pointInTime).to.equal(118);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[9];
        chai.expect(item.pointInTime).to.equal(200);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[10];
        chai.expect(item.pointInTime).to.equal(234);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(2);
    });

    it('Calculate number of concurrent working indexes - Single time range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([{ startTime: 2, endTime: 6 }]);

        finder.aggregate();
        const output = finder.workData;

        let item = output[0];
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(7);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(1);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(7);
        chai.expect(item.numberOfItems).to.equal(0);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(11);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(18);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(1);
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
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(14);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(71);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(3);
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
        chai.expect(item.pointInTime).to.equal(1);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(6);
        chai.expect(item.numberOfItems).to.equal(4);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(8);
        chai.expect(item.numberOfItems).to.equal(5);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(12);
        chai.expect(item.numberOfItems).to.equal(7);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(14);
        chai.expect(item.numberOfItems).to.equal(6);
        item = output[6];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(4);
        item = output[7];
        chai.expect(item.pointInTime).to.equal(22);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[8];
        chai.expect(item.pointInTime).to.equal(71);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[9];
        chai.expect(item.pointInTime).to.equal(72);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(7);
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
        chai.expect(item.pointInTime).to.equal(1);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(8);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(12);
        chai.expect(item.numberOfItems).to.equal(4);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(71);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(4);
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
        chai.expect(item.pointInTime).to.equal(0);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(3);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(4);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(11);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[6];
        chai.expect(item.pointInTime).to.equal(20);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[7];
        chai.expect(item.pointInTime).to.equal(28);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[8];
        chai.expect(item.pointInTime).to.equal(41);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[9];
        chai.expect(item.pointInTime).to.equal(44);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(4);
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
        chai.expect(item.pointInTime).to.equal(1);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(3);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(8);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(11);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(3);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(11);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(15);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(2);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(3);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(5);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(7);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(2);
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
        chai.expect(item.pointInTime).to.equal(2);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[1];
        chai.expect(item.pointInTime).to.equal(30);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[2];
        chai.expect(item.pointInTime).to.equal(41);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[3];
        chai.expect(item.pointInTime).to.equal(50);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[4];
        chai.expect(item.pointInTime).to.equal(61);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[5];
        chai.expect(item.pointInTime).to.equal(70);
        chai.expect(item.numberOfItems).to.equal(2);
        item = output[6];
        chai.expect(item.pointInTime).to.equal(81);
        chai.expect(item.numberOfItems).to.equal(1);
        item = output[7];
        chai.expect(item.pointInTime).to.equal(601);
        chai.expect(item.numberOfItems).to.equal(0);

        chai.expect(finder.maxConcurrentItems).to.equal(2);
    });

});   
