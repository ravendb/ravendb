import getLicenseLimitsUsage from "commands/licensing/getLicenseLimitsUsage";

export default class LicenseService {
    async getLimitsUsage() {
        return new getLicenseLimitsUsage().execute();
    }
}
