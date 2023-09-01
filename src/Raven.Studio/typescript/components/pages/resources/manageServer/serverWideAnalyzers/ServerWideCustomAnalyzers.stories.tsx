import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideCustomAnalyzers from "./ServerWideCustomAnalyzers";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer",
    component: ServerWideCustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideCustomAnalyzers>;

function commonInit() {
    const { manageServerService } = mockServices;

    manageServerService.withGetServerWideCustomAnalyzers();
}

export function WithNoLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Enterprise();

    return <ServerWideCustomAnalyzers />;
}

export function WithLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Community();

    return <ServerWideCustomAnalyzers />;
}
