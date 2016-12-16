import utils = require("utils");

var viewUnderTest = 'database/tasks/importDatabase';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/alerts/getGlobalAlertsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });
});

