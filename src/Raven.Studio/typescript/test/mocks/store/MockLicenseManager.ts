import { globalDispatch } from "components/storeCompat";
import { licenseActions } from "components/common/shell/licenseSlice";
import { LicenseStubs } from "test/stubs/LicenseStubs";

export class MockLicenseManager {
    with_License(override?: Partial<Raven.Server.Commercial.LicenseStatus>) {
        globalDispatch(licenseActions.statusLoaded({ ...LicenseStubs.getStatus(), ...override }));
    }

    with_LimitsUsage() {
        globalDispatch(licenseActions.limitsUsageLoaded(LicenseStubs.limitsUsage()));
    }
}
