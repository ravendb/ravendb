class apiKeyDatabase {
    tenantId = ko.observable<string>();
    admin = ko.observable<boolean>();
    readOnly = ko.observable<boolean>();
    tenantIdOrDefault: KnockoutComputed<string>;

    constructor(dto: apiKeyDatabaseDto) {
        this.tenantId(dto.TenantId);
        this.admin(dto.Admin);
        this.readOnly(dto.ReadOnly);

        this.tenantIdOrDefault = ko.computed(() => this.tenantId() ? this.tenantId() : "Select a database");
    }

    toDto(): apiKeyDatabaseDto {
        return {
            Admin: this.admin(),
            ReadOnly: this.readOnly(),
            TenantId: this.tenantId()
        };
    }
}

export = apiKeyDatabase;