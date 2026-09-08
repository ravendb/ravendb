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

            const paidFeatures: Partial<LicenseStatus> = {
                HasStudioConfiguration: true,
                HasPostgreSqlIntegration: true,
            };

            const licenseConfigs: Record<string, Partial<LicenseStatus>> = {
                "53f54157-3862-47b6-9dbd-94d323687a90": {},
                "53f54157-3862-47b6-9dbd-94d323687a91": { Type: "Essential" },
                "53f54157-3862-47b6-9dbd-94d323687a94": { Type: "Professional", MaxClusterSize: 5, ...paidFeatures },
                "53f54157-3862-47b6-9dbd-94d323687a92": { Type: "Enterprise", MaxClusterSize: 0, ...paidFeatures },
                "53f54157-3862-47b6-9dbd-94d323687a95": { Type: "EnterpriseAi", MaxClusterSize: 0, ...paidFeatures },
                "53f54157-3862-47b6-9dbd-94d323687a93": { Type: "Developer", MaxClusterSize: 5, ...paidFeatures },
            };

            const config = licenseConfigs[license.Id];
            return config
                ? { ...baseInfo, LicenseStatus: { ...baseInfo.LicenseStatus, ...config } }
                : baseInfo;
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
