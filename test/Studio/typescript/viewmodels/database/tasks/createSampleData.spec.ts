import utils = require("utils");

import collectionsStats = require("src/Raven.Studio/typescript/models/database/documents/collectionsStats");

var viewUnderTest = 'database/tasks/createSampleData';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {

        const collectionStatsDto: collectionsStatsDto = {
            NumberOfDocuments: 0,
            Collections: {
            }
        }
        const fakeDb = { name: "any" } as any;

        utils.mockCommand('commands/database/documents/getCollectionsStatsCommand', () => new collectionsStats(collectionStatsDto, fakeDb));

        utils.mockCommand('commands/database/studio/createSampleDataClassCommand', () => "c# code");

        return utils.mockActiveDatabase()
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });
});

