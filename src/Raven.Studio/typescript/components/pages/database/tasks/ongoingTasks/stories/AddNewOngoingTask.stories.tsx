import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import AddNewOngoingTask from "../AddNewOngoingTask";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import assertUnreachable from "components/utils/assertUnreachable";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

export default {
    title: "Pages/Tasks/Ongoing tasks/Add New Ongoing Task",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseType: {
            control: "radio",
            options: ["sharded", "cluster", "singleNode"],
        },
    },
} satisfies Meta;

type DatabaseType = "sharded" | "cluster" | "singleNode";

interface AddNewOngoingTaskStoryArgs {
    databaseType: DatabaseType;
    isAiOnly: boolean;
}

export const Default: StoryObj<AddNewOngoingTaskStoryArgs> = {
    name: "Add New Ongoing Task",
    render: (props) => {
        commonInit(props);

        return <AddNewOngoingTask queryParams={{ isAiOnly: props.isAiOnly }} />;
    },
    args: {
        isAiOnly: false,
        databaseType: "sharded",
    },
};

const commonInit = ({ databaseType }: AddNewOngoingTaskStoryArgs) => {
    const { accessManager, license, databases } = mockStore;
    const { tasksService, licenseService } = mockServices;

    license.with_License();

    switch (databaseType) {
        case "sharded":
            databases.withActiveDatabase_Sharded();
            break;
        case "cluster":
            databases.withActiveDatabase_NonSharded_Cluster();
            break;
        case "singleNode":
            databases.withActiveDatabase_NonSharded_SingleNode();
            break;
        default:
            assertUnreachable(databaseType);
    }

    accessManager.with_securityClearance("ClusterAdmin");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = 0;
        dto.OngoingTasks = [];
        dto.PullReplications = [];
    });

    licenseService.withLimitsUsage();
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
