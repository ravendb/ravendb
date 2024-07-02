import React from "react";
import { supportStatusArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { HelpAndResourcesWidget } from "components/common/helpAndResources/HelpAndResourcesWidget";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Bits/Help and resources",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        supportStatus: supportStatusArgType,
    },
} satisfies Meta;

interface HelpAndResourcesStoryArgs {
    licenseServerConnection: boolean;
    isCloud: boolean;
    supportStatus: Raven.Server.Commercial.Status;
}

export const HelpAndResourcesStory: StoryObj<HelpAndResourcesStoryArgs> = {
    name: "Help and resources",
    render: (args) => {
        const { licenseService } = mockServices;
        const { license } = mockStore;

        license.with_License({
            IsCloud: args.isCloud,
        });

        license.with_Support({
            Status: args.supportStatus,
        });

        if (args.licenseServerConnection) {
            licenseService.withConnectivityCheck();
        } else {
            licenseService.withConnectivityCheck({
                connected: false,
                exception: "Can't connect to api.ravendb.net",
            });
        }
        return <HelpAndResourcesWidget />;
    },
    args: {
        isCloud: false,
        supportStatus: "ProductionSupport",
        licenseServerConnection: true,
    },
};
