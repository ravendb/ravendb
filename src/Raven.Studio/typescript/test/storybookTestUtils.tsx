import { mockServices } from "./mocks/services/MockServices";
import React from "react";
import { configureMockServices, ServiceProvider } from "components/hooks/useServices";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { DirtyFlagProvider } from "components/hooks/useDirtyFlag";

export function storybookContainerPublicContainer(storyFn: any) {
    return (
        <div className="container">
            <div className="padding">{storyFn()}</div>
        </div>
    );
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
            <ServiceProvider services={mockServices.context}>
                <ChangesProvider changes={mockHooks.useChanges.mock}>{storyFn()}</ChangesProvider>
            </ServiceProvider>
        </DirtyFlagProvider>
    );
}

export function withBootstrap5(storyFn: any) {
    return (
        <>
            <div className="bs5" style={{ padding: "30px" }}>
                {storyFn()}
            </div>
            <style>{`body {overflow: auto !important;}`}</style>
        </>
    );
}

export const licenseArgType = {
    control: {
        type: "select",
        options: [
            "None",
            "Community",
            "Essential",
            "Professional",
            "Enterprise",
            "Developer",
        ] satisfies Raven.Server.Commercial.LicenseType[],
    },
};

export const databaseAccessArgType = {
    control: {
        type: "select",
        options: ["DatabaseAdmin", "DatabaseRead", "DatabaseReadWrite"] satisfies databaseAccessLevel[],
    },
};
