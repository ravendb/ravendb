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
            if (license.Id === "53f54157-3862-47b6-9dbd-94d323687a90") {
                return SetupWizardStubs.registrationInfoCommunity();
            }

            if (license.Id === "53f54157-3862-47b6-9dbd-94d323687a91") {
                return {
                    ...SetupWizardStubs.registrationInfoCommunity(),
                    LicenseType: "Essential",
                };
            }

            if (license.Id === "53f54157-3862-47b6-9dbd-94d323687a92") {
                return {
                    ...SetupWizardStubs.registrationInfoCommunity(),
                    LicenseType: "Enterprise",
                    MaxClusterSize: 2147483647,
                };
            }

            if (license.Id === "53f54157-3862-47b6-9dbd-94d323687a93") {
                return {
                    ...SetupWizardStubs.registrationInfoCommunity(),
                    LicenseType: "Developer",
                    MaxClusterSize: 5,
                };
            }

            if (license.Id === "53f54157-3862-47b6-9dbd-94d323687a94") {
                return {
                    ...SetupWizardStubs.registrationInfoCommunity(),
                    LicenseType: "Professional",
                    MaxClusterSize: 5,
                };
            }
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
