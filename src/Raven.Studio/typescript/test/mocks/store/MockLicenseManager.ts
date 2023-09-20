import { globalDispatch } from "components/storeCompat";
import { licenseActions } from "components/common/shell/licenseSlice";
import { LicenseStubs } from "test/stubs/LicenseStubs";

export class MockLicenseManager {
    with_License(override?: Partial<Raven.Server.Commercial.LicenseStatus>) {
        globalDispatch(licenseActions.statusLoaded({ ...LicenseStubs.getStatus(), ...override }));
    }

    with_LicenseLimited(override?: Partial<Raven.Server.Commercial.LicenseStatus>) {
        globalDispatch(licenseActions.statusLoaded({ ...LicenseStubs.getStatusLimited(), ...override }));
    }

    with_LimitsUsage(override?: Partial<Raven.Server.Commercial.LicenseLimitsUsage>) {
        globalDispatch(licenseActions.limitsUsageLoaded({ ...LicenseStubs.limitsUsage(), ...override }));
    }
}
