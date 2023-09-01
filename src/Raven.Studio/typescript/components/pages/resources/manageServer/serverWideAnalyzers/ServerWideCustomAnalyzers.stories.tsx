import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideCustomAnalyzers from "./ServerWideCustomAnalyzers";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Server-Wide Analyzers",
    component: ServerWideCustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideCustomAnalyzers>;

function commonInit() {
    const { manageServerService } = mockServices;

    manageServerService.withServerWideCustomAnalyzers();
}

export function NoLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Enterprise();

    return <ServerWideCustomAnalyzers />;
}

export function CommunityLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Community();

    return <ServerWideCustomAnalyzers />;
}
