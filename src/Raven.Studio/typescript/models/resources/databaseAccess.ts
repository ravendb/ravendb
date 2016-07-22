import shell = require("viewmodels/shell");
import resource = require("models/resources/resource");

class databaseAccess {

    admin = ko.observable<boolean>();
    tenantId = ko.observable<string>();
    readOnly = ko.observable<boolean>();
    searchResults: KnockoutComputed<string[]>;
    tenantCustomValidityError: KnockoutComputed<string>;

    static adminAccessType = 'Admin';
    static readWriteAccessType = 'Read, Write';
    static readOnlyAccessType = 'Read Only';
    static databaseAccessTypes =
        ko.observableArray<string>([databaseAccess.adminAccessType, databaseAccess.readWriteAccessType, databaseAccess.readOnlyAccessType]);

    currentAccessType = ko.computed({
        read:()=> {
            if (this.admin()) {
                return databaseAccess.adminAccessType;
            } else if (this.readOnly()) {
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
        this.tenantId(dto.TenantId != null ? dto.TenantId : '');

        this.searchResults = ko.computed(() => {
            var readOnly = shell.isGlobalAdmin() === false && shell.canReadWriteSettings() === false && shell.canReadSettings();
            if (readOnly)
                return [];

            var newResourceName: string = this.tenantId();
            var resourceNames = shell.resources().map((rs: resource) => rs.name)
                            .concat("*")
                            .filter((x, i, c) => c.indexOf(x) == i);
            return resourceNames.filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.tenantCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newTenantId = this.tenantId();
            var resourceNames = shell.resources().map((rs: resource) => rs.name)
                            .concat("*")
                            .filter((x, i, c) => c.indexOf(x) == i);
            var foundResource = resourceNames.first(name => newTenantId === name);

            if (!foundResource && newTenantId.length > 0) {
                errorMessage = "There is no database nor file system with such a name!";
            }

            return errorMessage;
        });
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
            TenantId: "",
            ReadOnly: false
        });
    }

    getTypes(): string[] {
        return databaseAccess.databaseAccessTypes();
    }
}
export = databaseAccess;
