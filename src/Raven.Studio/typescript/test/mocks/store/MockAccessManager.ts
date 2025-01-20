import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
import { globalDispatch } from "components/storeCompat";
import { accessManagerActions } from "components/common/shell/accessManagerSlice";

export class MockAccessManager {
    with_securityClearance(clearance: SecurityClearance) {
        globalDispatch(accessManagerActions.onSecurityClearanceSet(clearance));
    }

    with_databaseAccess(access: dictionary<databaseAccessLevel>) {
        globalDispatch(accessManagerActions.onDatabaseAccessLoaded(access));
    }

    with_isServerSecure(isSecureServer: boolean) {
        globalDispatch(accessManagerActions.onIsSecureServerSet(isSecureServer));
    }

    with_clientCertificateThumbprint(thumbprint: string) {
        globalDispatch(accessManagerActions.clientCertificateThumbprintSet(thumbprint));
    }
}
