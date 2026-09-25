import { getLicenseLimitWarning } from "./importLicenseLimits";
import { LicenseStubs } from "test/stubs/LicenseStubs";

describe("getLicenseLimitWarning", () => {
    it("returns null when the license has no limit", () => {
        expect(getLicenseLimitWarning(LicenseStubs.getStatus(), "subscriptions")).toBeNull();
    });

    it("returns null without a license status", () => {
        expect(getLicenseLimitWarning(null, "customSorters")).toBeNull();
    });

    it("names both limits of a limited license", () => {
        expect(getLicenseLimitWarning(LicenseStubs.getStatusLimited(), "subscriptions")).toBe(
            "Your license limits subscriptions to 3 per database and 15 per cluster. " +
                "Anything above the limit will fail to import."
        );
    });

    it("names only the limit that is defined", () => {
        const status: LicenseStatus = {
            ...LicenseStubs.getStatusLimited(),
            MaxNumberOfCustomAnalyzersPerDatabase: null,
        };
        expect(getLicenseLimitWarning(status, "customAnalyzers")).toBe(
            "Your license limits custom analyzers to 5 per cluster. Anything above the limit will fail to import."
        );
    });
});
