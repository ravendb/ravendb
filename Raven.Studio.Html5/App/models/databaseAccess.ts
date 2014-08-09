import shell = require("viewmodels/shell");
import database = require("models/database");

class databaseAccess {

    admin = ko.observable<boolean>();
    tenantId = ko.observable<string>();
    readOnly = ko.observable<boolean>();
    databaseNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    tenantCustomValidityError: KnockoutComputed<string>;

    static adminAccessType = 'Admin';
    static readWriteAccessType = 'Read, Write';
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

        this.databaseNames = ko.computed(() => shell.databases().map((db: database) => db.name).concat("*"));

        this.searchResults = ko.computed(() => {
            var newDatabaseName = this.tenantId();
            if (!newDatabaseName || newDatabaseName.length < 2) {
                return [];
            }
            return this.databaseNames().filter((name) => name.toLowerCase().indexOf(newDatabaseName.toLowerCase()) > -1);
        });

        this.tenantCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newTenantId = this.tenantId();
            var foundDb = this.databaseNames().first(name => newTenantId == name);

            if (!foundDb && newTenantId.length > 0) {
                errorMessage = "Database name doesn't exist!";
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