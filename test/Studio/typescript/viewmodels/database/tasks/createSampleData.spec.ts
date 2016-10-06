import utils = require("utils");

var viewUnderTest = 'database/tasks/createSampleData';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/database/studio/createSampleDataClassCommand', () => "c# code");
        return utils.runViewmodelTest(viewUnderTest, {});
    });
});

