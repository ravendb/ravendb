import SetupWizardService from "components/services/SetupWizardService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import { SetupWizardStubs } from "test/stubs/SetupWizardStubs";

export default class MockSetupWizardService extends AutoMockService<SetupWizardService> {
    constructor() {
        super(new SetupWizardService());
    }

    withEula(dto?: MockedValue<string>) {
        return this.mockResolvedValue(this.mocks.getEula, dto, SetupWizardStubs.eula());
    }

    withNodesInfoFromPackage(dto?: MockedValue<Raven.Server.Web.System.ConfigurationNodeInfo[]>) {
        return this.mockResolvedValue(
            this.mock.extractNodesInfoFromPackage,
            dto,
            SetupWizardStubs.nodesInfoFromPackage()
        );
    }

    withRegistrationInfo() {
        return this.mocks.registrationInfo.mockImplementation(async (license) => {
            const baseInfo = SetupWizardStubs.registrationInfoCommunity();

            const licenseConfigs: Record<string, Partial<typeof baseInfo>> = {
                "53f54157-3862-47b6-9dbd-94d323687a90": {},
                "53f54157-3862-47b6-9dbd-94d323687a91": { LicenseType: "Essential" },
                "53f54157-3862-47b6-9dbd-94d323687a94": { LicenseType: "Professional", MaxClusterSize: 5 },
                "53f54157-3862-47b6-9dbd-94d323687a92": { LicenseType: "Enterprise", MaxClusterSize: 2147483647 },
                "53f54157-3862-47b6-9dbd-94d323687a95": { LicenseType: "EnterpriseAi", MaxClusterSize: 2147483647 },
                "53f54157-3862-47b6-9dbd-94d323687a93": { LicenseType: "Developer", MaxClusterSize: 5 },
            };

            const config = licenseConfigs[license.Id];
            return config ? { ...baseInfo, ...config } : baseInfo;
        });
    }

    withHostsForCertificate(dto?: MockedValue<string[]>) {
        return this.mockResolvedValue(this.mock.listHostsForCertificate, dto, SetupWizardStubs.hostsForCertificate());
    }

    withGetSetupLocalNodeIps(dto?: MockedValue<string[]>) {
        return this.mockResolvedValue(this.mock.getSetupLocalNodeIps, dto, SetupWizardStubs.localNodeIps());
    }

    withGetSetupParameters(dto?: MockedValue<Raven.Server.Commercial.SetupParameters>) {
        return this.mockResolvedValue(this.mock.getSetupParameters, dto, SetupWizardStubs.setupParameters());
    }

    withGetIpsInfo(dto?: MockedValue<Raven.Server.Commercial.UserDomainsWithIps>) {
        return this.mockResolvedValue(this.mock.getIpsInfo, dto, SetupWizardStubs.ipsInfo());
    }

    withCheckDomainAvailability(dto?: MockedValue<domainAvailabilityResult>) {
        return this.mockResolvedValue(
            this.mock.checkDomainAvailability,
            dto,
            SetupWizardStubs.checkDomainAvailability()
        );
    }

    withClaimDomain(dto?: MockedValue<ClaimDomainResult>) {
        return this.mockResolvedValue(this.mock.claimDomain, dto, SetupWizardStubs.claimDomain());
    }

    withLetsEncryptAgreement() {
        return this.mockResolvedValue(
            this.mock.getLetsEncryptAgreement,
            undefined,
            SetupWizardStubs.letsEncryptAgreement()
        );
    }
}
