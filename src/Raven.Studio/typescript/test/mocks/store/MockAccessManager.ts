import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
import accessManager from "common/shell/accessManager";
import { globalDispatch } from "components/storeCompat";
import { onDatabaseAccessLoaded, onSecurityClearanceSet } from "components/common/shell/accessManagerSlice";

export class MockAccessManager {
    with_securityClearance(clearance: SecurityClearance) {
        accessManager.default.securityClearance(clearance);

        globalDispatch(onSecurityClearanceSet(clearance));
    }

    with_databaseAccess(access: dictionary<databaseAccessLevel>) {
        accessManager.databasesAccess = access;

        globalDispatch(onDatabaseAccessLoaded(access));
    }
}
