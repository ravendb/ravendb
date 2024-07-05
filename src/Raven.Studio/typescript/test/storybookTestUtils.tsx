import { mockServices } from "./mocks/services/MockServices";
import React from "react";
import { configureMockServices, ServiceProvider } from "components/hooks/useServices";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { DirtyFlagProvider } from "components/hooks/useDirtyFlag";
import { ConfirmDialogProvider } from "components/common/ConfirmDialog";

export function storybookContainerPublicContainer(storyFn: any) {
    return <div className="container">{storyFn()}</div>;
}

let needsTestMock = true;

if (needsTestMock) {
    configureMockServices(mockServices.context);
    needsTestMock = false;
}

export function forceStoryRerender() {
    return {
        key: new Date().toISOString(),
    };
}

export function withStorybookContexts(storyFn: any) {
    return (
        <DirtyFlagProvider setIsDirty={mockHooks.useDirtyFlag.mock}>
            <ConfirmDialogProvider>
                <ServiceProvider services={mockServices.context}>
                    <ChangesProvider changes={mockHooks.useChanges.mock}>{storyFn()}</ChangesProvider>
                </ServiceProvider>
            </ConfirmDialogProvider>
        </DirtyFlagProvider>
    );
}

export function withBootstrap5(storyFn: any) {
    return (
        <>
            <div
                className="bs5"
                style={{ padding: "30px", minHeight: "100vh", display: "flex", flexDirection: "column" }}
            >
                {storyFn()}
            </div>
            <style>{`body {overflow: auto !important;}`}</style>
        </>
    );
}

export function withForceRerender(storyFn: any) {
    return <React.Fragment {...forceStoryRerender()}>{storyFn()}</React.Fragment>;
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
