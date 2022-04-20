import { mockServices } from "./mocks/MockServices";
import React from "react";
import { ServiceProvider } from "../components/hooks/useServices";

export function storybookContainerPublicContainer(storyFn: any) {
    return (
        <div className="container">
            <div className="padding">{storyFn()}</div>
        </div>
    );
}

export function withStorybookContexts(storyFn: any) {
    return (
        <div style={{ margin: "50px" }}>
            <ServiceProvider services={mockServices.context}>{storyFn()}</ServiceProvider>
        </div>
    );
}
