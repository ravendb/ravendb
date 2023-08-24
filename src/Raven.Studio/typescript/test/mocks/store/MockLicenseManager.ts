import { globalDispatch } from "components/storeCompat";
import { licenseActions } from "components/common/shell/licenseSlice";
import { LicenseStubs } from "test/stubs/LicenseStubs";

export class MockLicenseManager {
    with_Enterprise() {
        globalDispatch(licenseActions.statusLoaded(LicenseStubs.enterprise()));
    }

    with_Community() {
        globalDispatch(licenseActions.statusLoaded(LicenseStubs.community()));
    }
}
