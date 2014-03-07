import databaseAccess = require("models/databaseAccess");

class windowsAuthData {

    name = ko.observable<string>();
    enabled = ko.observable<boolean>();
    databases = ko.observableArray<databaseAccess>();

    constructor(dto: windowsAuthDataDto) {
        this.name(dto.Name);
        this.enabled(dto.Enabled);
        this.databases(dto.Databases.map(dbDto => new databaseAccess(dbDto)));
    }

    toDto(): windowsAuthDataDto {
        return {
            Name: this.name(),
            Enabled: this.enabled(),
            Databases: this.databases().map(db => db.toDto())
        }
    }

    static empty(): windowsAuthData {
        return new windowsAuthData({
            Name: "[new settings]",
            Enabled: false,
            Databases: []
        });
    }

    enable() {
        this.enabled(true);
    }

    disable() {
        this.enabled(false);
    }

    addDatabase() {
        this.databases.push(databaseAccess.empty());
    }

    removeDatabase(dba: databaseAccess) {
        this.databases.remove(dba);
    }
}
export = windowsAuthData;