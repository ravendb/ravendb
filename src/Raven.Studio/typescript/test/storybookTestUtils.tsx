import { mockServices } from "./mocks/services/MockServices";
import React, { useState } from "react";
import { configureMockServices, ServiceProvider } from "components/hooks/useServices";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { createStoreConfiguration } from "components/store";
import { Provider } from "react-redux";

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
    // eslint-disable-next-line react-hooks/rules-of-hooks
    const [store] = useState(() => createStoreConfiguration());
    return (
        <div style={{ margin: "50px" }}>
            <Provider store={store}>
                <ServiceProvider services={mockServices.context}>
                    <ChangesProvider changes={mockHooks.useChanges.mock}>{storyFn()}</ChangesProvider>
                </ServiceProvider>
            </Provider>
        </div>
    );
}

export function withBootstrap5(storyFn: any) {
    return <div className="bs5">{storyFn()}</div>;
}
