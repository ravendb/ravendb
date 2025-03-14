import { mockServices } from "./mocks/services/MockServices";
import React from "react";
import { configureMockServices, ServiceProvider } from "components/hooks/useServices";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { DirtyFlagProvider } from "components/hooks/useDirtyFlag";
import { ConfirmDialogProvider } from "components/common/ConfirmDialog";
import { ReactRenderer } from "@storybook/react";
import { PartialStoryFn } from "storybook/internal/types";

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
        <DirtyFlagProvider setIsDirty={mockHooks.useDirtyFlag.mock}>
            <ConfirmDialogProvider>
                <ServiceProvider services={mockServices.context}>
                    <ChangesProvider changes={mockHooks.useChanges.mock}>
                        <Story />
                    </ChangesProvider>
                </ServiceProvider>
            </ConfirmDialogProvider>
        </DirtyFlagProvider>
    );
}

export function withBootstrap5(Story: StoryFunction) {
    return (
        <React.Fragment key="bs5">
            <div
                id="page-host"
                className="bs5"
                style={{ padding: "30px", minHeight: "100vh", display: "flex", flexDirection: "column" }}
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
