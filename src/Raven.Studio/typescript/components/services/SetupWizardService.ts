import getEulaCommand from "commands/licensing/getEulaCommand";
import continueUnsecureClusterConfigurationCommand from "commands/wizard/continueUnsecureClusterConfigurationCommand";
import extractNodesInfoFromPackageCommand from "commands/wizard/extractNodesInfoFromPackageCommand";
import finishSetupCommand from "commands/wizard/finishSetupCommand";
import listHostsForCertificateCommand from "commands/wizard/listHostsForCertificateCommand";
import registrationInfoCommand from "commands/wizard/registrationInfoCommand";
import getSetupLocalNodeIpsCommand from "commands/wizard/getSetupLocalNodeIpsCommand";
import getSetupParametersCommand from "commands/wizard/getSetupParametersCommand";
import getIpsInfoCommand from "commands/wizard/getIpsInfoCommand";
import checkDomainAvailabilityCommand from "commands/wizard/checkDomainAvailabilityCommand";
import claimDomainCommand from "commands/wizard/claimDomainCommand";
import continueSecureClusterConfigurationCommand from "commands/wizard/continueSecureClusterConfigurationCommand";
import loadAgreementCommand from "commands/wizard/loadAgreementCommand";

export default class SetupWizardService {
    async getEula() {
        return new getEulaCommand().execute();
    }

    async extractNodesInfoFromPackage(...args: ConstructorParameters<typeof extractNodesInfoFromPackageCommand>) {
        return new extractNodesInfoFromPackageCommand(...args).execute();
    }

    async registrationInfo(...args: ConstructorParameters<typeof registrationInfoCommand>) {
        return new registrationInfoCommand(...args).execute();
    }

    async listHostsForCertificate(...args: ConstructorParameters<typeof listHostsForCertificateCommand>) {
        return new listHostsForCertificateCommand(...args).execute();
    }

    async getSetupLocalNodeIps(...args: ConstructorParameters<typeof getSetupLocalNodeIpsCommand>) {
        return new getSetupLocalNodeIpsCommand(...args).execute();
    }

    async getSetupParameters(...args: ConstructorParameters<typeof getSetupParametersCommand>) {
        return new getSetupParametersCommand(...args).execute();
    }

    async getIpsInfo(...args: ConstructorParameters<typeof getIpsInfoCommand>) {
        return new getIpsInfoCommand(...args).execute();
    }
    async checkDomainAvailability(...args: ConstructorParameters<typeof checkDomainAvailabilityCommand>) {
        return new checkDomainAvailabilityCommand(...args).execute();
    }

    async claimDomain(...args: ConstructorParameters<typeof claimDomainCommand>) {
        return new claimDomainCommand(...args).execute();
    }

    async finishSetup(...args: ConstructorParameters<typeof finishSetupCommand>) {
        return new finishSetupCommand(...args).execute();
    }

    async continueSecureClusterConfiguration(
        ...args: ConstructorParameters<typeof continueSecureClusterConfigurationCommand>
    ) {
        return new continueSecureClusterConfigurationCommand(...args).execute();
    }

    async continueUnsecureClusterConfiguration(
        ...args: ConstructorParameters<typeof continueUnsecureClusterConfigurationCommand>
    ) {
        return new continueUnsecureClusterConfigurationCommand(...args).execute();
    }

    async getLetsEncryptAgreement(...args: ConstructorParameters<typeof loadAgreementCommand>) {
        return new loadAgreementCommand(...args).execute();
    }
}
