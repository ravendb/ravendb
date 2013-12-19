class apiKeyDatabase {
    tenantId = ko.observable<string>();
    admin = ko.observable<boolean>();
    readOnly = ko.observable<boolean>();

    constructor(dto: apiKeyDatabaseDto) {
        this.tenantId(dto.TenantId);
        this.admin(dto.Admin);
        this.readOnly(dto.ReadOnly);
    }
}

export = apiKeyDatabase;