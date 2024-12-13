import { composeStories } from "@storybook/react";
import * as stories from "./FeatureAvailabilitySummary.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { FeatureAvailabilitySummaryStory } = composeStories(stories);

describe("FeatureAvailabilitySummary", () => {
    it("can render", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(screen.getByText(/See which plans offer this and more exciting features/)).toBeInTheDocument();
    });

    it("can show 'Upgrade Instance' for cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory isCloud />);

        expect(screen.getByText(/Upgrade Instance/)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: /Cloud pricing/ })).toBeInTheDocument();
    });

    it("can show 'Upgrade License' for non-cloud license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory />);

        expect(screen.getByText(/Upgrade License/)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: /Pricing plans/ })).toBeInTheDocument();
    });

    it("shows 'Are you developing?' for Community license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Community" />);

        expect(screen.queryByText(/Are you developing?/)).toBeInTheDocument();
        expect(screen.queryByRole("link", { name: /Developer license/ })).toBeInTheDocument();
    });

    it("shows 'Are you developing?' for Professional license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Professional" />);

        expect(screen.queryByText(/Are you developing?/)).toBeInTheDocument();
        expect(screen.queryByRole("link", { name: /Developer license/ })).toBeInTheDocument();
    });

    it("doesn't show 'Are you developing?' for Developer license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Developer" />);

        expect(screen.queryByText(/Are you developing?/)).not.toBeInTheDocument();
        expect(screen.queryByRole("link", { name: /Developer license/ })).not.toBeInTheDocument();
    });

    it("doesn't show 'Are you developing?' for Enterprise license", () => {
        const { screen } = rtlRender(<FeatureAvailabilitySummaryStory licenseType="Enterprise" />);

        expect(screen.queryByText(/Are you developing?/)).not.toBeInTheDocument();
        expect(screen.queryByRole("link", { name: /Developer license/ })).not.toBeInTheDocument();
    });
});
