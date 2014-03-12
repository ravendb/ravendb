class databaseAccess {

    admin = ko.observable<boolean>();
    tenantId = ko.observable<string>();
    readOnly = ko.observable<boolean>();
    tenantIdOrDefault: KnockoutComputed<string>;

    constructor(dto: databaseAccessDto) {
        this.admin(dto.Admin);
        this.readOnly(dto.ReadOnly);
        this.tenantId(dto.TenantId);

        this.tenantIdOrDefault = ko.computed(() => this.tenantId() ? this.tenantId() : "Select a database");
    }

    toDto(): databaseAccessDto {
        return {
            Admin: this.admin(),
            TenantId: this.tenantId(),
            ReadOnly: this.readOnly()
        }
    }

    static empty(): databaseAccess {
        return new databaseAccess({
            Admin: false,
            TenantId: null,
            ReadOnly: false
        });
    }
}
export = databaseAccess;