import shell = require("viewmodels/shell");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class databaseAccess {

    admin = ko.observable<boolean>();
    tenantId = ko.observable<string>();
    readOnly = ko.observable<boolean>();
    databaseNames: KnockoutComputed<string[]>;
    fileSystemNames: KnockoutComputed<string[]>;
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
        this.tenantId(dto.TenantId != null ? dto.TenantId : '');

        this.databaseNames = ko.computed(() => shell.databases().map((db: database) => db.name));
        this.fileSystemNames = ko.computed(() => shell.fileSystems().map((fs: filesystem) => fs.name));

        this.searchResults = ko.computed(() => {
            var newDatabaseName: string = this.tenantId();

            var dbNames = this.databaseNames().filter((name) => name.toLowerCase().indexOf(newDatabaseName.toLowerCase()) > -1);
            var fsNames = this.fileSystemNames().filter((name) => name.toLowerCase().indexOf(newDatabaseName.toLowerCase()) > -1);
            return dbNames.concat(fsNames).concat("*");
        });

        this.tenantCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newTenantId = this.tenantId();
            var foundDb = this.databaseNames().first(name => newTenantId == name);
            var foundFs = this.fileSystemNames().first(name => newTenantId == name);

            if (newTenantId != "*" && !foundDb && !foundFs && newTenantId.length > 0) {
                errorMessage = "There is no database nor file system with such name!";
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