import utils = require("utils");

var viewUnderTest = 'database/documents/editDocument';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {
        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });
});
