import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideCustomSorters from "./ServerWideCustomSorters";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Server-Wide Sorters",
    component: ServerWideCustomSorters,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideCustomSorters>;

function commonInit() {
    const { manageServerService } = mockServices;

    manageServerService.withServerWideCustomSorters();
}

export function NoLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Enterprise();

    return <ServerWideCustomSorters />;
}

export function CommunityLimits() {
    commonInit();

    const { license } = mockStore;
    license.with_Community();

    return <ServerWideCustomSorters />;
}
