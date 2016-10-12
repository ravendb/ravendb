import utils = require("utils");

var viewUnderTest = 'database/transformers/transformers';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind empty list', () => {

        utils.mockCommand('commands/database/transformers/getTransformersCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

    it('should bind non-empty list', () => {

        utils.mockCommand('commands/database/transformers/getTransformersCommand', () => getSampleData());

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

});

function getSampleData(): Array<Raven.Abstractions.Indexing.TransformerDefinition> {
    return [
        {
            "Name": "t1",
            "TransformResults": "from result in results\r\nselect new {\r\n    result.A\r\n}",
            "LockMode": "LockedIgnore",
            "Temporary": false,
            "TransfomerId": 4
        },
        {
            "Name": "t2",
            "TransformResults": "from result in results \r\nselect new {\r\n    result.A\r\n}",
            "LockMode": "Unlock",
            "Temporary": false,
            "TransfomerId": 5
        }
    ]   
}