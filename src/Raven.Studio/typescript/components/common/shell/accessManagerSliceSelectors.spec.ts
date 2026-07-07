import { createStoreConfiguration } from "components/store";
import { accessManagerActions } from "components/common/shell/accessManagerSlice";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databasesSlice } from "components/common/shell/databasesSlice";
import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

interface CreateStoreArgs {
    databaseAccess: dictionary<databaseAccessLevel>;
    securityClearance?: SecurityClearance;
    activeDatabaseName?: string;
}

function createStoreWithAccess({
    databaseAccess,
    securityClearance = "ValidUser",
    activeDatabaseName = null,
}: CreateStoreArgs) {
    const store = createStoreConfiguration();
    store.dispatch(accessManagerActions.onDatabaseAccessLoaded(databaseAccess));
    store.dispatch(accessManagerActions.onSecurityClearanceSet(securityClearance));
    if (activeDatabaseName) {
        store.dispatch(databasesSlice.actions.activeDatabaseChanged(activeDatabaseName));
    }
    return store;
}

describe("accessManagerSliceSelectors", () => {
    const databaseAccess: dictionary<databaseAccessLevel> = {
        Northwind: "DatabaseAdmin",
        orders: "DatabaseRead",
    };

    describe("getEffectiveDatabaseAccessLevel", () => {
        it("can get access level when name casing matches the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const getLevel = accessManagerSelectors.getEffectiveDatabaseAccessLevel(store.getState());

            expect(getLevel("Northwind")).toEqual("DatabaseAdmin");
        });

        it("can get access level when name casing differs from the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const getLevel = accessManagerSelectors.getEffectiveDatabaseAccessLevel(store.getState());

            expect(getLevel("northwind")).toEqual("DatabaseAdmin");
            expect(getLevel("ORDERS")).toEqual("DatabaseRead");
        });

        it("can get access level for active database when name casing differs from the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess, activeDatabaseName: "northWIND" });
            const getLevel = accessManagerSelectors.getEffectiveDatabaseAccessLevel(store.getState());

            expect(getLevel()).toEqual("DatabaseAdmin");
        });

        it("returns no access level when database is not listed in the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const getLevel = accessManagerSelectors.getEffectiveDatabaseAccessLevel(store.getState());

            expect(getLevel("unknown")).toBeUndefined();
        });
    });

    describe("getHasDatabaseAdminAccess", () => {
        it("can check admin access when name casing differs from the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const hasAdminAccess = accessManagerSelectors.getHasDatabaseAdminAccess(store.getState());

            expect(hasAdminAccess("northwind")).toBe(true);
            expect(hasAdminAccess("ORDERS")).toBe(false);
        });
    });

    describe("getHasDatabaseWriteAccess", () => {
        it("can check write access when name casing differs from the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const hasWriteAccess = accessManagerSelectors.getHasDatabaseWriteAccess(store.getState());

            expect(hasWriteAccess("NORTHWIND")).toBe(true);
            expect(hasWriteAccess("Orders")).toBe(false);
        });
    });

    describe("getCanHandleOperation", () => {
        it("can check operation permission when name casing differs from the certificate", () => {
            const store = createStoreWithAccess({ databaseAccess });
            const canHandleOperation = accessManagerSelectors.getCanHandleOperation(store.getState());

            expect(canHandleOperation("DatabaseAdmin", "northwind")).toBe(true);
            expect(canHandleOperation("DatabaseRead", "ORDERS")).toBe(true);
            expect(canHandleOperation("DatabaseReadWrite", "Orders")).toBe(false);
        });
    });
});
