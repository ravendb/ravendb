import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import versioningEntry = require("models/filesystem/versioningEntry");
import getConfigurationByKeyCommand = require('commands/filesystem/getConfigurationByKeyCommand');

class getVersioningCommand extends commandBase {
    constructor(private fs: filesystem) {
        super();
    }

    /* TODO
    execute(): JQueryPromise<versioningEntry> {
        var result = $.Deferred();

        var getCommand = new getConfigurationByKeyCommand(this.fs, 'Raven/Versioning/DefaultConfiguration')
            .execute();

        getCommand.fail(xhr => result.fail(xhr));
        getCommand.done(config => {
            result.resolve(new versioningEntry(JSON.parse(config)));
        });

        return result;
    }*/
}

export = getVersioningCommand;
