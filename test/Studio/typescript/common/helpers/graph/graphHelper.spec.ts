import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import graphHelper = require("src/Raven.Studio/typescript/common/helpers/graph/graphHelper");
import chai = require("chai");

const helperUnderTest = 'common/helpers/graph/graphHelper';

describe(helperUnderTest, () => {

    it('Calculate shorten horizontal line left->right', () => {

        const result = graphHelper.shortenLine(1, 1, 10, 1, 2);
        
        chai.expect(result.x1).to.be.equal(3);
        chai.expect(result.y1).to.be.equal(1);
        chai.expect(result.x2).to.be.equal(8);
        chai.expect(result.y2).to.be.equal(1);
    });

    it('Calculate shorten horizontal line right->left', () => {

        const result = graphHelper.shortenLine(10, 1, 1, 1, 2);

        chai.expect(result.x1).to.be.equal(8);
        chai.expect(result.y1).to.be.equal(1);
        chai.expect(result.x2).to.be.equal(3);
        chai.expect(result.y2).to.be.equal(1);
    });

});   
