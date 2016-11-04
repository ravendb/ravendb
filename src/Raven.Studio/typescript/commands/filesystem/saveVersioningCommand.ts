import filesystem = require('models/filesystem/filesystem');
import commandBase = require("commands/commandBase");
import saveConfigurationCommand = require('commands/filesystem/saveConfigurationCommand');
import configurationKey = require('models/filesystem/configurationKey');

class saveVersioningCommand extends commandBase {
    /* TODO
    constructor(private fs: filesystem, private versioningEntry: versioningEntryDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return new saveConfigurationCommand(this.fs,
            new configurationKey(this.fs, 'Raven/Versioning/DefaultConfiguration'), this.versioningEntry)
          .execute();
    }*/
}

export = saveVersioningCommand;
