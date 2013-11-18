class apiKeyDatabase {
    name = ko.observable<string>();
    admin = ko.observable<boolean>();
    readOnly = ko.observable<boolean>();

    constructor(dto: apiKeyDatabaseDto) {
        this.name(dto.name);
        this.admin(dto.admin);
        this.readOnly(dto.readOnly);
    }
}

export = apiKeyDatabase;