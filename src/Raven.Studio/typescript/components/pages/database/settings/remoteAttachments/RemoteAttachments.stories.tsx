import { databaseAccessArgType, licenseArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { mockStore } from "test/mocks/store/MockStore";
import RemoteAttachments from "components/pages/database/settings/remoteAttachments/RemoteAttachments";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

interface RemoteAttachmentsStoryArgs {
    databaseAccess: databaseAccessLevel;
    hasRemoteAttachments: boolean;
    hasRemoteDestinations: boolean;
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
}

export default {
    title: "Pages/Settings/Remote Attachments",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/HDUBpGrDU7d5Bh8I0HE8Bl/Pages---Remote-attachments?node-id=17-2682&t=whTMznR6tx6dnFHi-0",
        },
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
        licenseType: licenseArgType,
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        hasRemoteAttachments: true,
        hasRemoteDestinations: true,
        licenseType: "Community",
        isCloud: false,
    },
    render: (args) => {
        const { accessManager, license, databases } = mockStore;
        const { databasesService } = mockServices;
        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        license.with_LicenseLimited({
            Type: args.licenseType,
            IsCloud: args.isCloud,
            HasRemoteAttachments: args.hasRemoteAttachments,
        });
        databasesService.withRemoteAttachmentsConfiguration(
            args.hasRemoteDestinations
                ? DatabasesStubs.remoteAttachmentsConfiguration()
                : DatabasesStubs.emptyRemoteAttachmentsConfiguration()
        );

        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        return <RemoteAttachments />;
    },
} satisfies Meta<RemoteAttachmentsStoryArgs>;

export const DefaultRemoteAttachments: StoryObj<RemoteAttachmentsStoryArgs> = {
    name: "Remote Attachments",
};

export const CloudProductionNotTurnedOn: StoryObj<RemoteAttachmentsStoryArgs> = {
    args: {
        licenseType: "Enterprise",
        isCloud: true,
        hasRemoteAttachments: false,
    },
};
