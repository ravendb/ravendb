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

    withGetChangeLog(dto?: MockedValue<Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse>) {
        const allBuilds = this.createValue(dto, LicenseStubs.changeLog());

        this.mocks.getChangeLog.mockImplementation(async (start: number, pageSize: number) => ({
            ...allBuilds,
            BuildCompatibilitiesForLatestMajorMinor: allBuilds.BuildCompatibilitiesForLatestMajorMinor.slice(
                start,
                start + pageSize
            ),
            BuildCompatibilitiesForUserMajorMinor: allBuilds.BuildCompatibilitiesForUserMajorMinor.slice(
                start,
                start + pageSize
            ),
        }));

        return allBuilds;
    }

    withLatestVersion(dto?: MockedValue<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>) {
        return this.mockResolvedValue(this.mocks.getLatestVersion, dto, LicenseStubs.latestVersion());
    }

    withVerifyLicense(dto?: MockedValue<DownloadFreeLicenseResponse>) {
        return this.mockResolvedValue(this.mocks.verifyLicense, dto, LicenseStubs.getLicenseFreeDownload());
    }
}
