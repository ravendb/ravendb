class databaseSetting {

    key = ko.observable<string>();
    value = ko.observable<string>();

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);
    }

    static empty(): databaseSetting {
        return new databaseSetting(null, null);
    }

}

export = databaseSetting;
