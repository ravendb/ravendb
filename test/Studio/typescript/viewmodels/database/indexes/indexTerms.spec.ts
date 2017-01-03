import utils = require("utils");

var viewUnderTest = 'database/indexes/indexTerms';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {

        utils.mockCommand('commands/database/index/getIndexEntriesFieldsCommand', () => ["Name"]);
        utils.mockCommand("commands/database/index/getIndexTermsCommand", () => getIndexTermsCommand());

        return utils.mockActiveDatabase()
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

});

function getIndexTermsCommand(): string[] {
    return ["100", "101", "102"];
}
