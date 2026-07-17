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
}

export const Default: StoryObj<ImportFromFileStoryArgs> = {
    name: "Import From File",
    render: (props) => {
        commonInit(props);

        return <ImportDatabaseFromFile />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        licenseType: "Enterprise",
    },
};

export const CommunityLicense: StoryObj<ImportFromFileStoryArgs> = {
    name: "License restricted (Community)",
    render: (props) => {
        commonInit(props);

        const { license } = mockStore;

        // getStatusLimited() doesn't zero these flags on its own - override explicitly so the
        // restricted-features alert (SelectFileSection) and disabled toggles (ConfigurationToImportSection)
        // actually render as restricted in the story.
        license.with_LicenseLimited({
            Type: props.licenseType,
            HasDocumentsCompression: false,
            HasDataArchival: false,
            HasTimeSeriesRollupsAndRetention: false,
            HasPostgreSqlIntegration: false,
            HasClientConfiguration: false,
        });

        return <ImportDatabaseFromFile />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        licenseType: "Community",
    },
};

const commonInit = ({ databaseAccess, licenseType }: ImportFromFileStoryArgs) => {
    const { accessManager, databases, license, collectionsTracker } = mockStore;

    const db = databases.withActiveDatabase_NonSharded_SingleNode();

    accessManager.with_databaseAccess({ [db.name]: databaseAccess });
    accessManager.with_securityClearance("ValidUser");
    license.with_LicenseLimited({ Type: licenseType });
    collectionsTracker.with_Collections();
};
