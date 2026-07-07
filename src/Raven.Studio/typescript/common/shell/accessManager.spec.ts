import accessManager from "common/shell/accessManager";

describe("accessManager", () => {
    beforeEach(() => {
        accessManager.databasesAccess = {
            Northwind: "DatabaseAdmin",
            orders: "DatabaseRead",
        };
        accessManager.default.securityClearance("ValidUser");
    });

    describe("getDatabasesAccess", () => {
        it("can get access level when name casing matches the certificate", () => {
            expect(accessManager.getDatabasesAccess("Northwind")).toEqual("DatabaseAdmin");
        });

        it("can get access level when name casing differs from the certificate", () => {
            expect(accessManager.getDatabasesAccess("northwind")).toEqual("DatabaseAdmin");
            expect(accessManager.getDatabasesAccess("Orders")).toEqual("DatabaseRead");
        });

        it("can get access level for a shard when name casing differs from the certificate", () => {
            expect(accessManager.getDatabasesAccess("NORTHWIND$1")).toEqual("DatabaseAdmin");
        });

        it("returns undefined when database is not listed in the certificate", () => {
            expect(accessManager.getDatabasesAccess("unknown")).toBeUndefined();
        });

        it("returns null when name is not provided", () => {
            expect(accessManager.getDatabasesAccess(null)).toBeNull();
        });
    });

    describe("getEffectiveDatabaseAccessLevel", () => {
        it("uses certificate access level regardless of name casing", () => {
            expect(accessManager.default.getEffectiveDatabaseAccessLevel("northWIND")).toEqual("DatabaseAdmin");
            expect(accessManager.default.getEffectiveDatabaseAccessLevel("ORDERS")).toEqual("DatabaseRead");
        });

        it("is admin for any database when user is operator or above", () => {
            accessManager.default.securityClearance("Operator");
            expect(accessManager.default.getEffectiveDatabaseAccessLevel("unknown")).toEqual("DatabaseAdmin");
        });
    });
});
