import {
    databaseAccessArgType,
    databaseArgType,
    DatabaseType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import ImportDataOptions from "../ImportDataOptions";
import { mockStore } from "test/mocks/store/MockStore";
import assertUnreachable from "components/utils/assertUnreachable";

export default {
    title: "Pages/Tasks/Import Data/Import Data Options",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseType: databaseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta;

interface ImportDataOptionsStoryArgs {
    databaseType: DatabaseType;
    databaseAccess: databaseAccessLevel;
}

export const Default: StoryObj<ImportDataOptionsStoryArgs> = {
    name: "Import Data Options",
    render: (props) => {
        commonInit(props);

        return <ImportDataOptions />;
    },
    args: {
        databaseType: "singleNode",
        databaseAccess: "DatabaseAdmin",
    },
};

const commonInit = ({ databaseType, databaseAccess }: ImportDataOptionsStoryArgs) => {
    const { accessManager, databases } = mockStore;

    let db;
    switch (databaseType) {
        case "sharded":
            db = databases.withActiveDatabase_Sharded();
            break;
        case "cluster":
            db = databases.withActiveDatabase_NonSharded_Cluster();
            break;
        case "singleNode":
            db = databases.withActiveDatabase_NonSharded_SingleNode();
            break;
        default:
            assertUnreachable(databaseType);
    }

    accessManager.with_databaseAccess({
        [db.name]: databaseAccess,
    });

    accessManager.with_securityClearance("ValidUser");
};
