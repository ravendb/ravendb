import utils = require("utils");

var viewUnderTest = 'database/documents/editDocument';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {
        return utils.runViewmodelTest(viewUnderTest, {});
    });
});
