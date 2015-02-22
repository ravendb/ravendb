import commandBase = require("commands/commandBase");
import database = require("models/database");
import versioningEntry = require("models/versioningEntry");
import configurationDocument = require("models/configurationDocument");

class getEffectiveVersioningsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocument<versioningEntry>[]> {
        var url = "/configuration/versioning";
        var mapper = (configs: configurationDocumentDto<versioningEntryDto>[]): configurationDocument<versioningEntry>[]=> {
            return configs.map(config =>
                configurationDocument.fromDtoWithTransform<versioningEntryDto, versioningEntry>(config, (x: versioningEntryDto) => new versioningEntry(x, true)));
        };
        return this.query<configurationDocument<versioningEntry>[]>(url, null, this.db, mapper);
    }

}

export = getEffectiveVersioningsCommand; 