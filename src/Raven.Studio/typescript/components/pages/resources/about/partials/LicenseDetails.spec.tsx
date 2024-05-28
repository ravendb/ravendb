import { forTesting } from "components/pages/resources/about/partials/LicenseDetails";

describe("LicenseDetails", function () {
    it("can filter by text", function () {
        const { filterFeatureAvailabilitySection, featureAvailabilityData } = forTesting;

        const featureToFind = "Read-Only Certificates";

        const filtered = filterFeatureAvailabilitySection(
            featureAvailabilityData,
            featureToFind,
            "showAll",
            ["community"],
            (feature, column) => feature[column].value
        );

        expect(filtered).toHaveLength(1);
        expect(filtered[0].items).toHaveLength(1);
        expect(filtered[0].items[0].name).toEqual(featureToFind);
    });

    it("can filter by differences", function () {
        const { filterFeatureAvailabilitySection, featureAvailabilityData } = forTesting;

        const filtered = filterFeatureAvailabilitySection(
            featureAvailabilityData,
            "",
            "showDiff",
            ["community", "enterprise", "professional"],
            (feature, column) => feature[column].value
        );

        expect(filtered.length).toBeGreaterThan(0);
    });
});
