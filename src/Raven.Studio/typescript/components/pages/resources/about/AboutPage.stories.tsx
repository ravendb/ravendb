import {
    licenseArgType,
    securityClearanceArgType,
    supportStatusArgType,
    withBootstrap5,
    withStorybookContexts,
    withForceRerender,
} from "test/storybookTestUtils";
import { ArgTypes, Meta, StoryObj } from "@storybook/react-webpack5";
import { AboutPage as AboutPageComponent } from "./AboutPage";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { ClusterStubs } from "test/stubs/ClusterStubs";
import moment from "moment";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";

export default {
    title: "Pages/About",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        securityClearance: securityClearanceArgType,
        licenseType: licenseArgType,
        supportStatus: supportStatusArgType,
    },
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/UTQSMvlXfMGANNKJvjfcUg/Pages---About?node-id=460-2357&t=NCFKh71z3moKzBcg-1",
        },
    },
} satisfies Meta<AboutPageStoryProps>;

interface AboutPageStoryProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
    licenseServerConnection: boolean;
    passiveServer: boolean;
    latestServerBuildNumber: number;
    isIsv: boolean;
    cloud: boolean;
    supportStatus: Raven.Server.Commercial.Status;
    subscriptionExpiration: string;
    expired: boolean;
    consentStatus: CheckConsentAiAssistantResultDto["Status"];
    usageStatus: CheckUsageAiAssistantResultDto["Status"];
    usagePercentage: number;
}

function commonInit(props: AboutPageStoryProps) {
    const { licenseService, aiAssistantService } = mockServices;
    const { license, accessManager, cluster, aiAssistant } = mockStore;

    aiAssistant.with_consent(props.consentStatus);
    aiAssistantService.withCheckConsent((dto) => (dto.Status = props.consentStatus));
    aiAssistantService.withCheckUsage({ Status: props.usageStatus, UsagePercentage: props.usagePercentage });

    accessManager.with_securityClearance(props.securityClearance);
    cluster.with_ClientVersion();
    cluster.with_ServerVersion();
    cluster.with_PassiveServer(props.passiveServer);
    license.with_License({
        Type: props.licenseType,
        IsIsv: props.isIsv,
        IsCloud: props.cloud,
        SubscriptionExpiration: props.subscriptionExpiration,
        Expired: props.expired,
    });
    license.with_Support({
        Status: props.supportStatus,
    });

    licenseService.withGetChangeLog();

    if (props.licenseServerConnection) {
        licenseService.withConnectivityCheck();
    } else {
        licenseService.withConnectivityCheck({
            connected: false,
            exception: "Can't connect to api.ravendb.net",
        });
    }

    licenseService.withLatestVersion((x) => {
        if (props.latestServerBuildNumber) {
            x.BuildNumber = props.latestServerBuildNumber;
        }
    });
    licenseService.withGetConfigurationSettings();
}

const defaultArgs: AboutPageStoryProps = {
    licenseType: "Enterprise",
    isIsv: false,
    cloud: false,
    passiveServer: false,
    supportStatus: "NoSupport",
    latestServerBuildNumber: undefined,
    licenseServerConnection: true,
    securityClearance: "ClusterAdmin",
    subscriptionExpiration: moment.utc().add(2, "month").format(),
    expired: false,
    consentStatus: "Success",
    usageStatus: "Success",
    usagePercentage: 35,
};

const defaultArgTypes: Partial<ArgTypes<AboutPageStoryProps>> = {
    consentStatus: {
        control: "select",
        options: ["Success", "InvalidCredentials", "ConsentRequired"],
    },
    usageStatus: {
        control: "select",
        options: ["Success", "InvalidCredentials"],
    },
};

const render = (props: AboutPageStoryProps) => {
    commonInit(props);

    return <AboutPageComponent />;
};

export const AboutPage: StoryObj<AboutPageStoryProps> = {
    render,
    args: defaultArgs,
    argTypes: defaultArgTypes,
};

export const ConnectionFailure: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseServerConnection: false,
    },
    argTypes: defaultArgTypes,
};

export const NoLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "None",
    },
    argTypes: defaultArgTypes,
};

export const DeveloperLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Developer",
    },
    argTypes: defaultArgTypes,
};

export const CommunityLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Community",
    },
    argTypes: defaultArgTypes,
};

export const ProfessionalLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Professional",
    },
    argTypes: defaultArgTypes,
};

export const EnterpriseLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Enterprise",
    },
    argTypes: defaultArgTypes,
};

export const EssentialLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Essential",
    },
    argTypes: defaultArgTypes,
};

export const NoSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "NoSupport",
    },
    argTypes: defaultArgTypes,
};

export const NoSupportCloud: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: true,
        supportStatus: "NoSupport",
    },
    argTypes: defaultArgTypes,
};

export const ProfessionalSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "ProfessionalSupport",
    },
    argTypes: defaultArgTypes,
};

export const ProductionSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "ProductionSupport",
    },
    argTypes: defaultArgTypes,
};

export const ProductionSupportCloud: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: true,
        supportStatus: "ProductionSupport",
    },
    argTypes: defaultArgTypes,
};

export const PartialSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "PartialSupport",
    },
    argTypes: defaultArgTypes,
};

export const UsingLatestVersion: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        latestServerBuildNumber: ClusterStubs.serverVersion().BuildVersion,
        cloud: false,
        supportStatus: "NoSupport",
    },
    argTypes: defaultArgTypes,
};

export const ExpiredLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        subscriptionExpiration: moment.utc().subtract(1, "month").format(),
        expired: true,
    },
    argTypes: defaultArgTypes,
};

export const AboutToExpireLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        subscriptionExpiration: moment.utc().add(3, "days").format(),
    },
    argTypes: defaultArgTypes,
};
