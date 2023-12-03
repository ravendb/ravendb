import {
    databaseAccessArgType,
    licenseArgType,
    statusArgType,
    withBootstrap5,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, Story, StoryObj } from "@storybook/react";
import { AboutPage } from "./AboutPage";
import React from "react";
import database from "models/resources/database";
import { boundCopy } from "components/utils/common";
import newVersionAvailableDetails from "viewmodels/common/notificationCenter/detailViewer/alerts/newVersionAvailableDetails";

export default {
    title: "Pages/About page",
    component: AboutPage,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        licenseType: licenseArgType,
        status: statusArgType,
        databaseAccess: databaseAccessArgType,
        newVersionAvailable: {
            options: ["6.0.2 (60002) - 12.11.2023", null],
            control: { type: "select" },
        },
    },
} satisfies Meta<typeof AboutPage>;

interface DefaultAboutPageProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    status: Raven.Server.Commercial.Status;
    licenseServerConnection: boolean;
    newVersionAvailable?: string;
    databaseAccess: databaseAccessLevel;
}

export const AboutTemplate: StoryObj<DefaultAboutPageProps> = {
    name: "About Page",
    render: ({
        licenseType,
        status,
        licenseServerConnection,
        newVersionAvailable,
        databaseAccess,
    }: DefaultAboutPageProps) => {
        return (
            <AboutPage
                licenseType={licenseType}
                status={status}
                licenseExpiration={Date()}
                supportId="5234067"
                licenseServerConnection={licenseServerConnection}
                newVersionAvailable={newVersionAvailable}
            />
        );
    },
    args: {
        licenseType: "Enterprise",
        status: "NoSupport",
        licenseServerConnection: true,
        databaseAccess: "DatabaseAdmin",
    },
};
