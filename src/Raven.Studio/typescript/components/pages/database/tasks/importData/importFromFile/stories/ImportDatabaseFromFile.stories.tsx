import {
    databaseAccessArgType,
    licenseArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import ImportDatabaseFromFile from "../ImportDatabaseFromFile";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Tasks/Import Data/Import From File",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseAccess: databaseAccessArgType,
        licenseType: licenseArgType,
    },
} satisfies Meta;

interface ImportFromFileStoryArgs {
    databaseAccess: databaseAccessLevel;
    licenseType: Raven.Server.Commercial.LicenseType;
    /** Every licence flag on, so no import row is licence-restricted. */
    hasAllLicenseFeatures: boolean;
    /** 0 documents + 0 indexes = empty database, so the overwrite warning is not shown */
    isEmptyDatabase: boolean;
}

function init({ databaseAccess, licenseType, hasAllLicenseFeatures, isEmptyDatabase }: ImportFromFileStoryArgs) {
    const { accessManager, databases, license, collectionsTracker } = mockStore;
    const { tasksService, databasesService } = mockServices;

    const db = databases.withActiveDatabase_NonSharded_SingleNode();

    accessManager.with_databaseAccess({ [db.name]: databaseAccess });
    accessManager.with_securityClearance("ValidUser");

    if (hasAllLicenseFeatures) {
        license.with_License({ Type: licenseType });
    } else {
        license.with_LicenseLimited({ Type: licenseType });
    }

    collectionsTracker.with_Collections();

    databasesService.withEssentialStats((dto) => {
        if (isEmptyDatabase) {
            dto.CountOfDocuments = 0;
            dto.CountOfIndexes = 0;
            dto.Indexes = [];
        }
    });

    tasksService.withImportDatabaseFromFile();
}

export const Default: StoryObj<ImportFromFileStoryArgs> = {
    name: "Import From File",
    render: (props) => {
        init(props);
        return <ImportDatabaseFromFile />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        licenseType: "Enterprise",
        hasAllLicenseFeatures: false,
        isEmptyDatabase: false,
    },
};

export const FullLicense: StoryObj<ImportFromFileStoryArgs> = {
    ...Default,
    name: "Import From File (all licence features)",
    args: {
        ...Default.args,
        hasAllLicenseFeatures: true,
    },
};
