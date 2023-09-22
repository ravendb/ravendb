import getClusterLicenseLimitsUsage from "commands/licensing/getClusterLicenseLimitsUsage";

export default class LicenseService {
    async getClusterLimitsUsage() {
        return new getClusterLicenseLimitsUsage().execute();
    }
}
