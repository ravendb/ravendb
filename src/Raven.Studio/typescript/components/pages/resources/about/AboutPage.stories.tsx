import {
    licenseArgType,
    securityClearanceArgType,
    supportStatusArgType,
    withBootstrap5,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { AboutPage as AboutPageComponent } from "./AboutPage";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { ClusterStubs } from "test/stubs/ClusterStubs";

export default {
    title: "Pages/About",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        securityClearance: securityClearanceArgType,
        licenseType: licenseArgType,
        supportStatus: supportStatusArgType,
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
}

function commonInit(props: AboutPageStoryProps) {
    const { licenseService } = mockServices;
    const { license, accessManager, cluster } = mockStore;

    accessManager.with_securityClearance(props.securityClearance);
    cluster.with_ClientVersion();
    cluster.with_ServerVersion();
    cluster.with_PassiveServer(props.passiveServer);
    license.with_License({
        Type: props.licenseType,
        IsIsv: props.isIsv,
        IsCloud: props.cloud,
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
};

const render = (props: AboutPageStoryProps) => {
    commonInit(props);

    return <AboutPageComponent />;
};

export const AboutPage: StoryObj<AboutPageStoryProps> = {
    render,
    args: defaultArgs,
};

export const ConnectionFailure: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseServerConnection: false,
    },
};

export const NoLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "None",
    },
};

export const DeveloperLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Developer",
    },
};

export const CommunityLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Community",
    },
};

export const ProfessionalLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Professional",
    },
};

export const EnterpriseLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Enterprise",
    },
};

export const EssentialLicense: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        licenseType: "Essential",
    },
};

export const NoSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "NoSupport",
    },
};

export const NoSupportCloud: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: true,
        supportStatus: "NoSupport",
    },
};

export const ProfessionalSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "ProfessionalSupport",
    },
};

export const ProductionSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "ProductionSupport",
    },
};

export const ProductionSupportCloud: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: true,
        supportStatus: "ProductionSupport",
    },
};

export const PartialSupportOnPremise: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        cloud: false,
        supportStatus: "PartialSupport",
    },
};

export const UsingLatestVersion: StoryObj<AboutPageStoryProps> = {
    render,
    args: {
        ...defaultArgs,
        latestServerBuildNumber: ClusterStubs.serverVersion().BuildVersion,
        cloud: false,
        supportStatus: "NoSupport",
    },
};
