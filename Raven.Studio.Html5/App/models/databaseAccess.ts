class databaseAccess {

    admin = ko.observable<boolean>();
    tenantId = ko.observable<string>();
    readOnly = ko.observable<boolean>();
    tenantIdOrDefault: KnockoutComputed<string>;

    static adminAccessType = 'Admin';
    static readWriteAccessType = 'Read,Write';
    static readOnlyAccessType = 'Read Only';
    static databaseAccessTypes =
        ko.observableArray<string>([databaseAccess.adminAccessType, databaseAccess.readWriteAccessType, databaseAccess.readOnlyAccessType]);


    

    currentAccessType = ko.computed({
        read:()=> {
            if (this.admin() === true) {
                return databaseAccess.adminAccessType;
            } else if (this.readOnly() === true) {
                return databaseAccess.readOnlyAccessType;
            }

            return databaseAccess.readWriteAccessType;
        },
        write:(value:string)=> {
            switch (value) {
                case databaseAccess.adminAccessType:
                    this.admin(true);
                    this.readOnly(false);
                    break;
                case databaseAccess.readOnlyAccessType:
                    this.admin(false);
                    this.readOnly(true);
                    break;
                case databaseAccess.readWriteAccessType:
                    this.admin(false);
                    this.readOnly(false);
                    break;
            default:
            }       
        }
    });

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
        };
    }

    static empty(): databaseAccess {
        return new databaseAccess({
            Admin: false,
            TenantId: null,
            ReadOnly: false
        });
    }

    getTypes(): string[] {
        return databaseAccess.databaseAccessTypes();
    }

}
export = databaseAccess;