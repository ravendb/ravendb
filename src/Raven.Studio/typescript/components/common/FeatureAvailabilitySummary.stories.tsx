import { licenseArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityValueData } from "./FeatureAvailabilitySummary";
import React from "react";
import IconName from "typings/server/icons";
import { AboutViewAnchored } from "components/common/AboutView";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Bits/Feature Availability Summary",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=3-12166&t=ipd4AuT8v0is7yTp-4",
        },
    },
} satisfies Meta;

interface FeatureAvailabilitySummaryStoryArgs {
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    isUnlimited: boolean;
    hasFeature: boolean;
    featureIcon: IconName;
    featureName: string;
    community: FeatureAvailabilityValueData;
    professional: FeatureAvailabilityValueData;
    enterprise: FeatureAvailabilityValueData;
}

export const FeatureAvailabilitySummaryStory: StoryObj<FeatureAvailabilitySummaryStoryArgs> = {
    name: "Feature Availability Summary",
    render: (args) => {
        const { license } = mockStore;

        license.with_License({
            Type: args.licenseType,
            IsCloud: args.isCloud,
            CanSetupDefaultRevisionsConfiguration: args.hasFeature,
        });

        return (
            <AboutViewAnchored defaultOpen="licensing">
                <FeatureAvailabilitySummaryWrapper
                    isUnlimited={args.isUnlimited}
                    data={[
                        {
                            featureName: "Default Policy",
                            featureIcon: args.featureIcon,
                            community: args.community,
                            professional: args.professional,
                            enterprise: args.enterprise,
                        },
                    ]}
                />
            </AboutViewAnchored>
        );
    },
    args: {
        licenseType: "Community",
        isCloud: false,
        isUnlimited: false,
        featureIcon: "default",
        hasFeature: false,
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
    argTypes: {
        licenseType: licenseArgType,
    },
};
