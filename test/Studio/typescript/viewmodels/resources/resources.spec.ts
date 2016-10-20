import utils = require("utils");

import resourcesInfo = require("src/Raven.Studio/typescript/models/resources/info/resourcesInfo");

var viewUnderTest = 'resources/resources';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/resources/getResourcesCommand', () => new resourcesInfo(getResourcesData()));

        return utils.runViewmodelTest(viewUnderTest, {});
    });
});

function getResourcesData(): Raven.Client.Data.ResourcesInfo {
    return {
        "Databases": [
            {
                "Bundles": ["Replication"],
                "IsAdmin": true,
                "Name": "sample",
                "Disabled": false,
                "TotalSize": {
                    "HumaneSize": "80.4 GBytes",
                    "SizeInBytes": 86328842649.6
                },
                "Errors": 5,
                "Alerts": 7,
                "UpTime": null,
                "BackupInfo": {
                    "BackupInterval": "7.00:00:00",
                    "LastBackup": "10.00:00:00"
                },
                "DocumentsCount": 10234,
                "IndexesCount": 30,
                "RejectClients": true,
                "IndexingStatus": null
            },
            {
                "Bundles": [],
                "IsAdmin": true,
                "Name": "sample2",
                "Disabled": true,
                "TotalSize": {
                    "HumaneSize": "80.4 GBytes",
                    "SizeInBytes": 86328842649.6
                },
                "Errors": 0,
                "Alerts": 0,
                "UpTime": "05:00:00",
                "BackupInfo": null,
                "DocumentsCount": 10234,
                "IndexesCount": 30,
                "RejectClients": false,
                "IndexingStatus": "Running"
            }
        ],
        Filesystems: []
    };
}