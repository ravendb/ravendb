import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./FeatureAvailabilitySummary.stories";
import { rtlRender } from "test/rtlTestUtils";
import { FeatureAvailabilityFootnote } from "./FeatureAvailabilitySummary";

type LicenseType = Raven.Server.Commercial.LicenseType;

const { FeatureAvailabilitySummaryStory } = composeStories(stories);

const selectors = {
    developerLicenseLink: /Developer license/,
    ifYouDevelopingText: /If you are developing, you can test this and many more features/,
};

describe("FeatureAvailabilitySummary", () => {
    it("can render", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(
            screen.getByText(/See which plans include this feature and other exciting features/)
        ).toBeInTheDocument();
    });

    it("can show 'Cloud pricing' for cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory isCloud />);

        expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
    });

    it("can show 'See full comparison' for non-cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(screen.getByRole("link", { name: /See full comparison/ })).toBeInTheDocument();
    });

    it.each(["None", "Community", "Essential", "Professional", "Enterprise", "EnterpriseAi"] satisfies LicenseType[])(
        `can show '${selectors.ifYouDevelopingText}' for %s license`,
        (licenseType) => {
            const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType={licenseType} />);

            expect(screen.queryByText(selectors.ifYouDevelopingText)).toBeInTheDocument();
            expect(screen.queryByRole("link", { name: selectors.developerLicenseLink })).toBeInTheDocument();
        }
    );

    it("can show footnotes numbered in order of appearance", () => {
        const embeddedFootnote: FeatureAvailabilityFootnote = {
            title: "Available connectors",
            items: [{ label: "Embedded" }],
        };
        const allFootnote: FeatureAvailabilityFootnote = {
            title: "Available connectors",
            items: [{ label: "OpenAI" }, { label: "Embedded" }],
        };

        const { screen } = rtlRender(
            <FeatureAvailabilitySummaryStory
                community={{ value: true, footnote: embeddedFootnote }}
                professional={{ value: true, footnote: embeddedFootnote }}
                enterprise={{ value: true, footnote: allFootnote }}
            />
        );

        expect(screen.getByText("Available connectors: Embedded").previousElementSibling).toHaveTextContent("1");
        expect(screen.getByText("Available connectors: OpenAI, Embedded").previousElementSibling).toHaveTextContent(
            "2"
        );
    });

    it(`can hide '${selectors.ifYouDevelopingText}' for Developer license`, () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Developer" />);

        expect(screen.queryByText(selectors.ifYouDevelopingText)).not.toBeInTheDocument();
        expect(screen.queryByRole("link", { name: selectors.developerLicenseLink })).not.toBeInTheDocument();
    });
});
