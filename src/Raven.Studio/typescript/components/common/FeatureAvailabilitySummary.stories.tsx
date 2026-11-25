import { licenseArgType, withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import FeatureAvailabilitySummaryWrapper, {
    CloudOptionalFeature,
    FeatureAvailabilityValueData,
} from "./FeatureAvailabilitySummary";
import React from "react";
import IconName from "typings/server/icons";
import { AboutViewAnchored } from "components/common/AboutView";
import { mockStore } from "test/mocks/store/MockStore";

interface FeatureAvailabilitySummaryStoryArgs {
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    isUnlimited: boolean;
    hasFeature: boolean;
    featureIcon: IconName;
    featureName: string;
    cloudOptionalFeature?: CloudOptionalFeature;
    community: FeatureAvailabilityValueData;
    professional: FeatureAvailabilityValueData;
    enterprise: FeatureAvailabilityValueData;
    quill: FeatureAvailabilityValueData;
}

export default {
    title: "Bits/Feature Availability Summary",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=3-12166&t=ipd4AuT8v0is7yTp-4",
        },
    },
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
                            featureName: args.featureName,
                            featureIcon: args.featureIcon,
                            cloudOptionalFeature: args.cloudOptionalFeature,
                            community: args.community,
                            professional: args.professional,
                            enterprise: args.enterprise,
                            quill: args.quill,
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
        featureName: "Default Policy",
        hasFeature: false,
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
        quill: { value: true },
    },
    argTypes: {
        licenseType: licenseArgType,
        cloudOptionalFeature: {
            control: "select",
            options: [
                undefined,
                "dataGovernance",
                "olapEtl",
                "queueEtl",
                "queueSink",
                "snowflakeEtl",
                "powerBi",
                "ai",
            ] satisfies (CloudOptionalFeature | undefined)[],
        },
    },
} satisfies Meta<FeatureAvailabilitySummaryStoryArgs>;

type Story = StoryObj<FeatureAvailabilitySummaryStoryArgs>;

export const FeatureAvailabilitySummaryStory: Story = {
    name: "Feature Availability Summary",
};

export const CloudFree: Story = {
    args: {
        isCloud: true,
        featureIcon: "remote-attachment",
        featureName: "Remote Attachments",
        cloudOptionalFeature: "dataGovernance",
        professional: { value: false },
    },
};

export const CloudProductionFeatureNotTurnedOn: Story = {
    args: {
        licenseType: "Enterprise",
        isCloud: true,
        featureIcon: "remote-attachment",
        featureName: "Remote Attachments",
        cloudOptionalFeature: "dataGovernance",
        professional: { value: false },
        enterprise: { value: true, overwrittenValue: false },
    },
};

export const CloudProductionFeatureTurnedOn: Story = {
    args: {
        licenseType: "Enterprise",
        isCloud: true,
        featureIcon: "remote-attachment",
        featureName: "Remote Attachments",
        cloudOptionalFeature: "dataGovernance",
        professional: { value: false },
    },
};

export const CloudProductionFeatureNotTurnedOnUnnamed: Story = {
    args: {
        licenseType: "Enterprise",
        isCloud: true,
        enterprise: { value: true, overwrittenValue: false },
    },
};
