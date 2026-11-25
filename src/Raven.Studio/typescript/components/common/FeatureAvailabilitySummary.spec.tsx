import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./FeatureAvailabilitySummary.stories";
import { rtlRender } from "test/rtlTestUtils";
import { FeatureAvailabilityFootnote } from "./FeatureAvailabilitySummary";

type LicenseType = Raven.Server.Commercial.LicenseType;

const {
    FeatureAvailabilitySummaryStory,
    CloudFree,
    CloudProductionFeatureNotTurnedOn,
    CloudProductionFeatureTurnedOn,
    CloudProductionFeatureNotTurnedOnUnnamed,
} = composeStories(stories);

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

    describe("cloud optional feature", () => {
        it("can show how to turn on the feature when it is not turned on in Production", () => {
            const { screen } = rtlRender(<CloudProductionFeatureNotTurnedOn />);

            expect(screen.getByText("Not turned on")).toBeInTheDocument();
            expect(
                screen.getByText(
                    "Turn on Data Governance in Cloud Portal (Product → Features). On a yearly plan, contact support."
                )
            ).toBeInTheDocument();
            expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
        });

        it("can show a general hint when the feature has no Cloud feature name", () => {
            const { screen } = rtlRender(<CloudProductionFeatureNotTurnedOnUnnamed />);

            expect(
                screen.getByText(
                    "Turn the feature on in Cloud Portal (Product → Features). On a yearly plan, contact support."
                )
            ).toBeInTheDocument();
        });

        it("can hide the hint when the feature is turned on", () => {
            const { screen } = rtlRender(<CloudProductionFeatureTurnedOn />);

            expect(screen.queryByText("Not turned on")).not.toBeInTheDocument();
            expect(screen.queryByText(/yearly plan, contact support/)).not.toBeInTheDocument();
            expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
        });

        it("can hide the hint on the Free plan", () => {
            const { screen } = rtlRender(<CloudFree />);

            expect(screen.queryByText("Not turned on")).not.toBeInTheDocument();
            expect(screen.queryByText(/yearly plan, contact support/)).not.toBeInTheDocument();
            expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
        });
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

    describe("footnotes", () => {
        const embeddedFootnote: FeatureAvailabilityFootnote = {
            title: "Available connectors",
            items: [{ label: "Embedded" }],
        };
        const allFootnote: FeatureAvailabilityFootnote = {
            title: "Available connectors",
            items: [{ label: "OpenAI" }, { label: "Embedded" }],
        };

        const renderWithFootnotes = () =>
            rtlRender(
                <FeatureAvailabilitySummaryStory
                    community={{ value: true, footnote: embeddedFootnote }}
                    professional={{ value: true, footnote: embeddedFootnote }}
                    enterprise={{ value: true, footnote: allFootnote }}
                />
            );

        it("can show footnotes numbered in order of appearance", () => {
            const { screen } = renderWithFootnotes();

            expect(screen.getByText("Available connectors: Embedded").previousElementSibling).toHaveTextContent("1");
            expect(screen.getByText("Available connectors: OpenAI, Embedded").previousElementSibling).toHaveTextContent(
                "2"
            );
        });

        it("can describe footnote marks with their footnotes", () => {
            const { screen } = renderWithFootnotes();

            screen
                .getAllByText("1", { selector: "sup" })
                .forEach((mark) => expect(mark).toHaveAccessibleDescription("Available connectors: Embedded"));
            screen
                .getAllByText("2", { selector: "sup" })
                .forEach((mark) => expect(mark).toHaveAccessibleDescription("Available connectors: OpenAI, Embedded"));
        });
    });

    it(`can hide '${selectors.ifYouDevelopingText}' for Developer license`, () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Developer" />);

        expect(screen.queryByText(selectors.ifYouDevelopingText)).not.toBeInTheDocument();
        expect(screen.queryByRole("link", { name: selectors.developerLicenseLink })).not.toBeInTheDocument();
    });
});
