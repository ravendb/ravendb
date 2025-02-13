import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import ServerWideCustomSorters from "./ServerWideCustomSorters";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Manage Server/Server-Wide Sorters",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/XQZ5bMEDkhXfDfoDyfcFdW/Pages---Server-Wide-Sorters?node-id=0-1&t=UvOZXHSoP6QvT2Zx-1",
        },
    },
} satisfies Meta;

interface DefaultServerWideCustomSortersProps {
    isEmpty: boolean;
    hasServerWideCustomSorters: boolean;
}

export const ServerWideCustomSortersStory: StoryObj<DefaultServerWideCustomSortersProps> = {
    name: "Server-Wide Sorters",
    render: (props: DefaultServerWideCustomSortersProps) => {
        const { manageServerService } = mockServices;

        manageServerService.withServerWideCustomSorters(
            props.isEmpty ? [] : ManageServerStubs.serverWideCustomSorters()
        );

        const { license } = mockStore;
        license.with_LicenseLimited({
            HasServerWideCustomSorters: props.hasServerWideCustomSorters,
        });

        return <ServerWideCustomSorters />;
    },
    args: {
        isEmpty: false,
        hasServerWideCustomSorters: true,
    },
};
