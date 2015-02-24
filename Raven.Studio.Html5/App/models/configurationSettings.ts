import configurationSetting = require("models/configurationSetting");

class configurationSettings {

    results: { [key: string]: configurationSetting }  = {};

    constructor(dto: configurationSettingsDto) {
        for (var key in dto.Results) {
            this.results[key] = new configurationSetting(dto.Results[key]);
        }
    }

}

export = configurationSettings;