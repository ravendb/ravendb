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

function getIndexTermsCommand(): Raven.Server.Documents.Queries.TermsQueryResult {
    return {
        "IndexName": "Orders/ByCompany",
        "ResultEtag": 5818580220936548876,
        "Terms": ["100", "101", "102"],
        NotModified: null
    }
}
