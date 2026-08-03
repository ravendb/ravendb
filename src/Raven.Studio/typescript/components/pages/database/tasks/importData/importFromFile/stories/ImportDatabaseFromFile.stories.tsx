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
}

interface StoryOptions {
    /** 0 documents + 0 indexes = empty database */
    isEmptyDatabase?: boolean;
}

function init(
    { databaseAccess, licenseType }: ImportFromFileStoryArgs,
    { isEmptyDatabase = false }: StoryOptions = {}
) {
    const { accessManager, databases, license, collectionsTracker } = mockStore;
    const { tasksService, databasesService } = mockServices;

    const db = databases.withActiveDatabase_NonSharded_SingleNode();

    accessManager.with_databaseAccess({ [db.name]: databaseAccess });
    accessManager.with_securityClearance("ValidUser");
    license.with_LicenseLimited({ Type: licenseType });
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

const story = (name: string, description: string, options: StoryOptions = {}): StoryObj<ImportFromFileStoryArgs> => ({
    name,
    render: (props) => {
        init(props, options);
        return <ImportDatabaseFromFile />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        licenseType: "Enterprise",
    },
    parameters: { docs: { description: { story: description } } },
});

export const Story00_Default = story(
    "00 - Default (everything available)",
    "Enterprise + DatabaseAdmin + non-sharded, database has data. EXPECT: no license/sharding/access " +
        "chips under the file input; every toggle enabled; the orange overwrite warning IS shown; " +
        '"Customize" panels start collapsed (nothing is restricted).'
);

export const Story01a_EmptyDatabase = story(
    "01a - Empty database (no overwrite warning)",
    "CountOfDocuments = 0 and CountOfIndexes = 0. EXPECT: the " +
        '"Importing will overwrite any existing documents and indexes" alert is ABSENT - nothing can be overwritten.',
    { isEmptyDatabase: true }
);
