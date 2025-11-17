import { mockServices } from "./mocks/services/MockServices";
import React from "react";
import { configureMockServices } from "components/hooks/useServices";
import { ReactRenderer } from "@storybook/react-webpack5";
import { PartialStoryFn } from "storybook/internal/types";
import { MockProviders } from "./rtlTestUtils";

type StoryFunction = PartialStoryFn<
    ReactRenderer,
    {
        [x: string]: any;
    }
>;

export function storybookContainerPublicContainer(Story: StoryFunction) {
    return (
        <div className="container">
            <Story />
        </div>
    );
}

let needsTestMock = true;

if (needsTestMock) {
    configureMockServices(mockServices.context);
    needsTestMock = false;
}

function forceStoryRerender() {
    return {
        key: new Date().toISOString(),
    };
}

export function withStorybookContexts(Story: StoryFunction) {
    return (
        <MockProviders>
            <Story />
        </MockProviders>
    );
}

const storyTopBarHeight = 40;

export function withBootstrap5(Story: StoryFunction) {
    return (
        <React.Fragment key="bs5">
            <div
                id="page-host"
                className="bs5"
                style={{
                    padding: "30px",
                    height: `calc(100vh - ${storyTopBarHeight}px)`,
                    display: "flex",
                    flexDirection: "column",
                }}
            >
                <Story />
            </div>
            <style>{`body {overflow: auto !important;}`}</style>
        </React.Fragment>
    );
}

export function withForceRerender(Story: StoryFunction) {
    const { key, ...rest } = forceStoryRerender();
    return (
        <React.Fragment key={key} {...rest}>
            <Story />
        </React.Fragment>
    );
}

export const licenseArgType = {
    control: {
        type: "select",
    },
    options: [
        "None",
        "Community",
        "Essential",
        "Professional",
        "Enterprise",
        "Developer",
    ] satisfies Raven.Server.Commercial.LicenseType[],
} as const;

export const supportStatusArgType = {
    control: {
        type: "select",
    },
    options: [
        "NoSupport",
        "PartialSupport",
        "ProductionSupport",
        "ProfessionalSupport",
        "LicenseNotFound",
    ] satisfies Raven.Server.Commercial.Status[],
} as const;

export const databaseAccessArgType = {
    control: {
        type: "select",
    },
    options: ["DatabaseAdmin", "DatabaseRead", "DatabaseReadWrite"] satisfies databaseAccessLevel[],
} as const;

export const securityClearanceArgType = {
    control: {
        type: "select",
    },
    options: [
        "Operator",
        "ClusterAdmin",
        "ClusterNode",
        "ValidUser",
        "UnauthenticatedClients",
    ] satisfies Raven.Client.ServerWide.Operations.Certificates.SecurityClearance[],
} as const;
