import {
    databaseAccessArgType,
    databaseArgType,
    DatabaseType,
    licenseArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import AddNewOngoingTask from "../AddNewOngoingTask";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import assertUnreachable from "components/utils/assertUnreachable";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

export default {
    title: "Pages/Tasks/Ongoing tasks/Add New Ongoing Task",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseType: databaseArgType,
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta;

interface AddNewOngoingTaskStoryArgs {
    databaseType: DatabaseType;
    licenseType: Raven.Server.Commercial.LicenseType;
    databaseAccess: databaseAccessLevel;
}

export const Default: StoryObj<AddNewOngoingTaskStoryArgs> = {
    name: "Add New Ongoing Task",
    render: (props) => {
        commonInit(props);

        return <AddNewOngoingTask />;
    },
    args: {
        databaseType: "sharded",
        licenseType: "Enterprise",
        databaseAccess: "DatabaseAdmin",
    },
};

const commonInit = ({ databaseType, licenseType, databaseAccess }: AddNewOngoingTaskStoryArgs) => {
    const { accessManager, license, databases } = mockStore;
    const { tasksService } = mockServices;

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

    license.with_LicenseLimited({
        Type: licenseType,
    });

    accessManager.with_securityClearance("ValidUser");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = 0;
        dto.OngoingTasks = [];
        dto.PullReplications = [];
    });

    tasksService.withGetSubscriptionTaskInfo();
    tasksService.withGetSubscriptionConnectionDetails();
    tasksService.withGetExternalReplicationProgress((dto) => {
        dto.Results = [];
    });
    tasksService.withGetEtlProgress((dto) => {
        dto.Results = [];
    });
    tasksService.withGetInternalReplicationProgress((dto) => {
        dto.Results = [];
    });
};
