import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import configurationSettings = require('models/database/globalConfig/configurationSettings');

class getConfigurationSettingsCommand extends commandBase {

    constructor(private db: database, private keys: Array<string>) {
        super();
    }

    execute(): JQueryPromise<configurationSettings> {
        var url = "/configuration/settings";//TODO: use endpoints
        var args = {
            key: this.keys
        };
        return this.query<any>(url, args, this.db, dto => new configurationSettings(dto));
    }
}

export = getConfigurationSettingsCommand;
