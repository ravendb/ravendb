import databaseAccess = require("models/resources/databaseAccess");

class windowsAuthData {

    name = ko.observable<string>();
    enabled = ko.observable<boolean>();
    databases = ko.observableArray<databaseAccess>();
    nameCustomValidity = ko.observable<string>('');
    needToShowSystemDatabaseWarning: KnockoutComputed<boolean>;
    invalidName = ko.observable<boolean>(false);
    verificationInProgress = ko.observable<boolean>(false);

    constructor(dto: windowsAuthDataDto) {
        this.name(dto.Name);
        this.enabled(dto.Enabled);
        this.databases(dto.Databases.map(dbDto => new databaseAccess(dbDto)));

        this.needToShowSystemDatabaseWarning = ko.computed(() => {
            var resources = this.databases();
            var hasAllDatabasesAdminAccess = resources.filter(x => x.admin() && x.tenantId() === "*").length > 0;
            var hasSystemDatabaseAdminAcces = resources.filter(x => x.admin() && x.tenantId() === "<system>").length > 0;
            return hasAllDatabasesAdminAccess && hasSystemDatabaseAdminAcces === false;
        });
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
            Name: "",
            Enabled: false,
            Databases: [databaseAccess.empty().toDto()]
        });
    }

    enable() {
        this.enabled(true);
    }

    disable() {
        this.enabled(false);
    }

    addEmptyDatabase() {
        this.databases.push(databaseAccess.empty());
    }

    removeDatabase(dba: databaseAccess) {
        this.databases.remove(dba);
    }
}
export = windowsAuthData;
