import LicenseService from "components/services/LicenseService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import { LicenseStubs } from "test/stubs/LicenseStubs";

export default class MockLicenseService extends AutoMockService<LicenseService> {
    constructor() {
        super(new LicenseService());
    }

    withLimitsUsage(dto?: MockedValue<Raven.Server.Commercial.LicenseLimitsUsage>) {
        return this.mockResolvedValue(this.mocks.getLimitsUsage, dto, LicenseStubs.limitsUsage());
    }
}
