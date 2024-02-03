import LicenseService from "components/services/LicenseService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import { LicenseStubs } from "test/stubs/LicenseStubs";

export default class MockLicenseService extends AutoMockService<LicenseService> {
    constructor() {
        super(new LicenseService());
    }

    withLimitsUsage(dto?: MockedValue<Raven.Server.Commercial.LicenseLimitsUsage>) {
        return this.mockResolvedValue(this.mocks.getClusterLimitsUsage, dto, LicenseStubs.limitsUsage());
    }

    withGetConfigurationSettings(dto?: MockedValue<Raven.Server.Config.Categories.LicenseConfiguration>) {
        return this.mockResolvedValue(this.mocks.getConfigurationSettings, dto, LicenseStubs.configurationSettings());
    }

    withConnectivityCheck(dto?: MockedValue<{ connected: boolean; exception: string }>) {
        return this.mockResolvedValue(
            this.mocks.checkLicenseServerConnectivity,
            dto,
            LicenseStubs.licenseServerConnectivityValid()
        );
    }

    withLatestVersion(dto?: MockedValue<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>) {
        return this.mockResolvedValue(this.mocks.getLatestVersion, dto, LicenseStubs.latestVersion());
    }
}
