import { mockServices } from "./mocks/services/MockServices";
import React from "react";
import { ServiceProvider } from "components/hooks/useServices";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";

export function storybookContainerPublicContainer(storyFn: any) {
    return (
        <div className="container">
            <div className="padding">{storyFn()}</div>
        </div>
    );
}

export function forceStoryRerender() {
    return {
        key: new Date().toISOString(),
    };
}

export function withStorybookContexts(storyFn: any) {
    return (
        <div style={{ margin: "50px" }}>
            <ServiceProvider services={mockServices.context}>
                <ChangesProvider changes={mockHooks.useChanges.mock}>{storyFn()}</ChangesProvider>
            </ServiceProvider>
        </div>
    );
}

export function withBootstrap5(storyFn: any) {
    return <div className="bs5">{storyFn()}</div>;
}
