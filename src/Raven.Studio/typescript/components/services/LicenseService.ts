import getClusterLicenseLimitsUsage from "commands/licensing/getClusterLicenseLimitsUsage";
import getLatestVersionInfoCommand from "commands/version/getLatestVersionInfoCommand";
import getConnectivityToLicenseServerCommand from "commands/licensing/getConnectivityToLicenseServerCommand";
import getLicenseConfigurationSettingsCommand from "commands/licensing/getLicenseConfigurationSettingsCommand";
import forceLicenseUpdateCommand from "commands/licensing/forceLicenseUpdateCommand";
import getLicenseStatusCommand from "commands/licensing/getLicenseStatusCommand";
import getChangeLogCommand from "commands/licensing/getChangeLogCommand";

export default class LicenseService {
    async getLicenseStatus() {
        return new getLicenseStatusCommand().execute();
    }
    async getClusterLimitsUsage() {
        return new getClusterLicenseLimitsUsage().execute();
    }

    async getChangeLog(page: number, perPage: number) {
        return new getChangeLogCommand(page, perPage).execute();
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
}
