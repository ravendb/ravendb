import getClusterLicenseLimitsUsage from "commands/licensing/getClusterLicenseLimitsUsage";
import getLatestVersionInfoCommand from "commands/version/getLatestVersionInfoCommand";
import getConnectivityToLicenseServerCommand from "commands/licensing/getConnectivityToLicenseServerCommand";
import getLicenseConfigurationSettingsCommand from "commands/licensing/getLicenseConfigurationSettingsCommand";
import forceLicenseUpdateCommand from "commands/licensing/forceLicenseUpdateCommand";
import getLicenseStatusCommand from "commands/licensing/getLicenseStatusCommand";
import getChangeLogCommand from "commands/licensing/getChangeLogCommand";
import licenseSendVerificationCodeCommand from "commands/licensing/licenseSendVerificationCodeCommand";
import licenseVerifyLicenseCommand from "commands/licensing/licenseVerifyLicenseCommand";

export default class LicenseService {
    async getLicenseStatus() {
        return new getLicenseStatusCommand().execute();
    }
    async getClusterLimitsUsage() {
        return new getClusterLicenseLimitsUsage().execute();
    }

    async getChangeLog(page: number, pageSize: number) {
        return new getChangeLogCommand(page, pageSize).execute();
    }

    async getLatestVersion(refresh: boolean = false) {
        return new getLatestVersionInfoCommand(refresh).execute();
    }

    async checkLicenseServerConnectivity() {
        return new getConnectivityToLicenseServerCommand().execute().then((result) => {
            return {
                connected: result.StatusCode === "OK",
                exception: result.Exception,
            };
        });
    }

    async getConfigurationSettings() {
        return new getLicenseConfigurationSettingsCommand().execute();
    }

    async forceUpdate() {
        return new forceLicenseUpdateCommand().execute();
    }

    async sendVerificationCode(...args: ConstructorParameters<typeof licenseSendVerificationCodeCommand>) {
        return new licenseSendVerificationCodeCommand(...args).execute();
    }

    async verifyLicense(...args: ConstructorParameters<typeof licenseVerifyLicenseCommand>) {
        return new licenseVerifyLicenseCommand(...args).execute();
    }
}
