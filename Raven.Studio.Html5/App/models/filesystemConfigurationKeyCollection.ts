import app = require("durandal/app");
import router = require("plugins/router");

import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import resource = require("models/resource");
import filesystemCollection = require("models/filesystemCollection");
import getFilesystemConfigurationCommand = require("commands/getFilesystemConfigurationCommand");

class filesystemConfigurationKeyCollection extends filesystemCollection {

    fetchItems(skip: number, take: number): JQueryPromise<pagedResultSet> {

        var command = new getFilesystemConfigurationCommand(this.owner);
        return command.execute()
                      .then(x => new pagedResultSet(x, x.count()));
    }

}

export = filesystemConfigurationKeyCollection;