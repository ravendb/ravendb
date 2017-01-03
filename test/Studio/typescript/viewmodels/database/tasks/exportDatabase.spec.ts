import utils = require("utils");

import collectionsStats = require("src/Raven.Studio/typescript/models/database/documents/collectionsStats");

var viewUnderTest = 'database/tasks/exportDatabase';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        const collectionStatsDto: collectionsStatsDto = {
            NumberOfDocuments: 100,
            Collections: {
                Orders: 50,
                Customers: 50
            }
        }
        const fakeDb = { name: "any" } as any;

        utils.mockCommand('commands/database/documents/getCollectionsStatsCommand', () => new collectionsStats(collectionStatsDto, fakeDb));
        utils.mockCommand('commands/alerts/getGlobalAlertsCommand', () => []);

        return utils.mockActiveDatabase()
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });
});

