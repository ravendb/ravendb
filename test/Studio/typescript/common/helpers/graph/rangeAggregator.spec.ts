import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import rangeAggregator = require("src/Raven.Studio/typescript/common/helpers/graph/rangeAggregator");
import chai = require("chai");

const helperUnderTest = 'common/helpers/graph/rangeAggregator';

describe(helperUnderTest, () => {

    it('Same time', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 2 },
            { startTime: 15, endTime: 15 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(3);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(16);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(1);
    });

    it('Start equals end', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 14 },
            { startTime: 15, endTime: 36 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(37);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(1);
    });

    it('Duplicate values', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 2, endTime: 6 },
            { startTime: 8, endTime: 10 },
            { startTime: 8, endTime: 10 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(7);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(8);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(11);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(2);
    });

    it('Many ranges', () => {

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
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(0);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(20);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(25);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(61);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(66);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[6];
        chai.expect(Item.pointInTime).to.equal(90);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[7];
        chai.expect(Item.pointInTime).to.equal(97);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[8];
        chai.expect(Item.pointInTime).to.equal(118);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[9];
        chai.expect(Item.pointInTime).to.equal(200);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[10];
        chai.expect(Item.pointInTime).to.equal(234);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(2);
    });

    it('Single range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([{ startTime: 2, endTime: 6 }]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(7);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(1);
    });

    it('Two different Ranges', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 11, endTime: 17 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(7);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(11);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(18);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(1);
    });

    it('Same start points', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 5, endTime: 70 },
            { startTime: 5, endTime: 13 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(14);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(71);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(3);
    });

    it('Same start & end points', () => {

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
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(1);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(6);
        chai.expect(Item.numberOfIndexesWorking).to.equal(4);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(8);
        chai.expect(Item.numberOfIndexesWorking).to.equal(5);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(12);
        chai.expect(Item.numberOfIndexesWorking).to.equal(7);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(14);
        chai.expect(Item.numberOfIndexesWorking).to.equal(6);
        Item = output[6];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(4);
        Item = output[7];
        chai.expect(Item.pointInTime).to.equal(22);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[8];
        chai.expect(Item.pointInTime).to.equal(71);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[9];
        chai.expect(Item.pointInTime).to.equal(72);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(7);
    });

    it('Same end points', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 8, endTime: 70 },
            { startTime: 1, endTime: 70 },
            { startTime: 12, endTime: 70 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(1);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(8);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(12);
        chai.expect(Item.numberOfIndexesWorking).to.equal(4);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(71);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(4);
    });

    it('Overlap3', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 20, endTime: 40 },
            { startTime: 2, endTime: 10 },
            { startTime: 3, endTime: 27 },
            { startTime: 0, endTime: 43 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(0);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(3);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(4);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(11);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[6];
        chai.expect(Item.pointInTime).to.equal(20);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[7];
        chai.expect(Item.pointInTime).to.equal(28);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[8];
        chai.expect(Item.pointInTime).to.equal(41);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[9];
        chai.expect(Item.pointInTime).to.equal(44);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(4);
    });

    it('Overlap2', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 2, endTime: 10 },
            { startTime: 1, endTime: 7 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(1);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(3);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(8);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(11);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(3);
    });

    it('Overlap1', () => {
        
        let finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 5, endTime: 14 },
            { startTime: 2, endTime: 10 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(11);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(15);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(2);
    });

    it('Range within range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 6 },
            { startTime: 3, endTime: 4 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(3);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(5);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(7);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(2);
    });

    it('Many ranges within range', () => {

        const finder = new rangeAggregator([]);

        finder.inputItems([
            { startTime: 2, endTime: 600 },
            { startTime: 30, endTime: 40 },
            { startTime: 50, endTime: 60 },
            { startTime: 70, endTime: 80 }
        ]);

        finder.aggregate();
        const output = finder.indexesWork;

        let Item = output[0];
        chai.expect(Item.pointInTime).to.equal(2);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[1];
        chai.expect(Item.pointInTime).to.equal(30);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[2];
        chai.expect(Item.pointInTime).to.equal(41);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[3];
        chai.expect(Item.pointInTime).to.equal(50);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[4];
        chai.expect(Item.pointInTime).to.equal(61);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[5];
        chai.expect(Item.pointInTime).to.equal(70);
        chai.expect(Item.numberOfIndexesWorking).to.equal(2);
        Item = output[6];
        chai.expect(Item.pointInTime).to.equal(81);
        chai.expect(Item.numberOfIndexesWorking).to.equal(1);
        Item = output[7];
        chai.expect(Item.pointInTime).to.equal(601);
        chai.expect(Item.numberOfIndexesWorking).to.equal(0);

        chai.expect(finder.getMaxConcurrentIndexes()).to.equal(2);
    });

});   
