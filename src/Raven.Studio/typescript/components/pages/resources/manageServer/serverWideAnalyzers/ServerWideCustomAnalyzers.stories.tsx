import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import { mockStore } from "test/mocks/store/MockStore";
import ServerWideCustomAnalyzers from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzers";

export default {
    title: "Pages/Manage Server/Server-Wide Analyzers",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface DefaultServerWideCustomAnalyzersProps {
    isEmpty: boolean;
    hasServerWideCustomAnalyzers: boolean;
}

export const ServerWideCustomAnalyzersStory: StoryObj<DefaultServerWideCustomAnalyzersProps> = {
    name: "Server-Wide Analyzers",
    render: (props: DefaultServerWideCustomAnalyzersProps) => {
        const { manageServerService } = mockServices;

        manageServerService.withServerWideCustomAnalyzers(
            props.isEmpty ? [] : ManageServerStubs.serverWideCustomAnalyzers()
        );

        const { license } = mockStore;
        license.with_LicenseLimited({
            HasServerWideAnalyzers: props.hasServerWideCustomAnalyzers,
        });

        return <ServerWideCustomAnalyzers />;
    },
    args: {
        isEmpty: false,
        hasServerWideCustomAnalyzers: true,
    },
};
