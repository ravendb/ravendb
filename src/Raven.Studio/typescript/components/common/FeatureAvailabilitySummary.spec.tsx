import { composeStories } from "@storybook/react";
import * as stories from "./FeatureAvailabilitySummary.stories";
import { rtlRender } from "test/rtlTestUtils";

type LicenseType = Raven.Server.Commercial.LicenseType;

const { FeatureAvailabilitySummaryStory } = composeStories(stories);

const selectors = {
    developerLicenseLink: /Developer license/,
    areYouDevelopingText: /Are you developing?/,
};

describe("FeatureAvailabilitySummary", () => {
    it("can render", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(screen.getByText(/See which plans offer this and more exciting features/)).toBeInTheDocument();
    });

    it("can show 'Cloud pricing' for cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory isCloud />);

        expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
    });

    it("can show 'See full comparison' for non-cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(screen.getByRole("link", { name: /See full comparison/ })).toBeInTheDocument();
    });

    it.each(["None", "Community", "Essential", "Professional"] satisfies LicenseType[])(
        "can show 'Are you developing?' for %s license",
        (licenseType) => {
            const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType={licenseType} />);

            expect(screen.queryByText(selectors.areYouDevelopingText)).toBeInTheDocument();
            expect(screen.queryByRole("link", { name: selectors.developerLicenseLink })).toBeInTheDocument();
        }
    );

    it.each(["Developer", "Enterprise"] satisfies LicenseType[])(
        "can hide 'Are you developing?' for %s license",
        (licenseType) => {
            const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType={licenseType} />);

            expect(screen.queryByText(selectors.areYouDevelopingText)).not.toBeInTheDocument();
            expect(screen.queryByRole("link", { name: selectors.developerLicenseLink })).not.toBeInTheDocument();
        }
    );
});
