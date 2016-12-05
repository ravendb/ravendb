import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import rangeAggregator = require("src/Raven.Studio/typescript/common/helpers/graph/rangeAggregator");
import chai = require("chai");

const helperUnderTest = 'common/helpers/graph/rangeAggregator';

describe(helperUnderTest, () => {

    it('shoud find single range', () => {

        const finder = new rangeAggregator();

        finder.pushRange(1, 10);

        const output = finder.aggregation;

        chai.expect(output.length).to.equal(1);

        const firstItem = output[0];
        chai.expect(firstItem.start).to.equal(1);
        chai.expect(firstItem.end).to.equal(10);
        chai.expect(firstItem.value).to.equal(1);
    });

    it('should aggregate 2 disjoint ranges', () => {
        const finder = new rangeAggregator();

        finder.pushRange(1, 10);
        finder.pushRange(11, 20);

        const output = finder.aggregation;

        chai.expect(output.length).to.equal(3);

        chai.expect(output[0]).to.deep.equal({
            start: 1,
            end: 10,
            value: 1
        });

        chai.expect(output[1]).to.deep.equal({
            start: 10,
            end: 11,
            value: 0
        });

        chai.expect(output[2]).to.deep.equal({
            start: 11,
            end: 20,
            value: 1
        });
    });

    it('should aggregate 2 overlapping ranges', () => {
        const finder = new rangeAggregator();

        finder.pushRange(1, 10);
        finder.pushRange(5, 20);

        const output = finder.aggregation;

        chai.expect(output.length).to.equal(3);

        chai.expect(output[0]).to.deep.equal({
            start: 1,
            end: 5,
            value: 1
        });

        chai.expect(output[1]).to.deep.equal({
            start: 5,
            end: 10,
            value: 2
        });

        chai.expect(output[2]).to.deep.equal({
            start: 10,
            end: 20,
            value: 1
        });
    });

});
