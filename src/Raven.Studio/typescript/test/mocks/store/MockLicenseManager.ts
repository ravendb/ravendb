import { globalDispatch } from "components/storeCompat";
import { licenseActions } from "components/common/shell/licenseSlice";
import { LicenseStubs } from "test/stubs/LicenseStubs";
import { mockServices } from "test/mocks/services/MockServices";

export class MockLicenseManager {
    with_License(override?: Partial<LicenseStatus>) {
        globalDispatch(licenseActions.statusLoaded({ ...LicenseStubs.getStatus(), ...override }));
    }

    with_LicenseLimited(override?: Partial<LicenseStatus>) {
        globalDispatch(licenseActions.statusLoaded({ ...LicenseStubs.getStatusLimited(), ...override }));
    }

    with_LimitsUsage(override?: Partial<Raven.Server.Commercial.LicenseLimitsUsage>) {
        const { licenseService } = mockServices;

        licenseService.withLimitsUsage({ ...LicenseStubs.limitsUsage(), ...override });
        globalDispatch(licenseActions.limitsUsageLoaded({ ...LicenseStubs.limitsUsage(), ...override }));
    }

    with_Support(override?: Partial<Raven.Server.Commercial.LicenseSupportInfo>) {
        globalDispatch(licenseActions.supportLoaded({ ...LicenseStubs.support(), ...override }));
    }
}
