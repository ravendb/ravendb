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
    isCloud: boolean;
    isEnabled: boolean;
    isIsv: boolean;
    status: Raven.Server.Commercial.Status;
    licenseServerConnection: boolean;
    newVersionAvailable?: string;
    databaseAccess: databaseAccessLevel;
    canUpgrade: boolean;
}

export const AboutTemplate: StoryObj<DefaultAboutPageProps> = {
    name: "About Page",
    render: ({
        licenseType,
        isCloud,
        isEnabled,
        isIsv,
        status,
        licenseServerConnection,
        newVersionAvailable,
        canUpgrade,
        databaseAccess,
    }: DefaultAboutPageProps) => {
        return (
            <AboutPage
                licenseType={licenseType}
                isCloud={isCloud}
                isEnabled={isEnabled}
                isIsv={isIsv}
                status={status}
                licenseExpiration={Date()}
                licenseServerConnection={licenseServerConnection}
                newVersionAvailable={newVersionAvailable}
                currentVersion="6.0.2 (60002) - 12.11.2023"
                canUpgrade={canUpgrade}
            />
        );
    },
    args: {
        licenseType: "Enterprise",
        isCloud: false,
        isEnabled: true,
        isIsv: false,
        status: "NoSupport",
        licenseServerConnection: true,
        databaseAccess: "DatabaseAdmin",
    },
};
