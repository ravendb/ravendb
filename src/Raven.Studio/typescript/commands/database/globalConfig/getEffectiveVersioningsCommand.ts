import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import versioningEntry = require("models/database/documents/versioningEntry");
import configurationDocument = require("models/database/globalConfig/configurationDocument");

class getEffectiveVersioningsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Versioning.VersioningConfiguration> {
        return super.execute();
    }

    /* TODO:

    execute(): JQueryPromise<configurationDocument<versioningEntry>[]> {
        var url = "/configuration/versioning";//TODO: use endpoints
        var mapper = (configs: configurationDocumentDto<versioningEntryDto>[]): configurationDocument<versioningEntry>[]=> {
            return configs.map(config =>
                configurationDocument.fromDtoWithTransform<versioningEntryDto, versioningEntry>(config, (x: versioningEntryDto) => new versioningEntry(x, true)));
        };
        return this.query<configurationDocument<versioningEntry>[]>(url, null, this.db, mapper);
    }*/

}

export = getEffectiveVersioningsCommand; 
