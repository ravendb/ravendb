class configurationSetting {

    localExists = ko.observable<boolean>();
    globalExists = ko.observable<boolean>();
    effectiveValue = ko.observable<string>();
    globalValue = ko.observable<string>();

    constructor(dto: configurationSettingDto) {
        this.localExists(dto.LocalExists);
        this.globalExists(dto.GlobalExists);
        this.effectiveValue(dto.EffectiveValue);
        this.globalValue(dto.GlobalValue);
    }

    copyFromGlobal() {
        if (this.globalExists()) {
            this.localExists(false);
            this.effectiveValue(this.globalValue());
        }
    }

}

export = configurationSetting;