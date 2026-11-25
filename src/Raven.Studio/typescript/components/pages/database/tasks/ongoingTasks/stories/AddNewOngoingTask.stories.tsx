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
import { TasksStubs } from "test/stubs/TasksStubs";

export default {
    title: "Pages/Tasks/Ongoing tasks/Add New Ongoing Task",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseType: databaseArgType,
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
    render: (props) => {
        commonInit(props);

        return <AddNewOngoingTask queryParams={{ isAiOnly: props.isAiOnly }} />;
    },
    args: {
        isAiOnly: false,
        databaseType: "sharded",
        licenseType: "EnterpriseAi",
        isCloud: false,
        hasOptionalFeatures: true,
        databaseAccess: "DatabaseAdmin",
        hasSubscriptionLimits: false,
        subscriptionsClusterUsage: 0,
        subscriptionsDatabaseCount: 0,
    },
} satisfies Meta<AddNewOngoingTaskStoryArgs>;

interface AddNewOngoingTaskStoryArgs {
    databaseType: DatabaseType;
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    hasOptionalFeatures: boolean;
    databaseAccess: databaseAccessLevel;
    isAiOnly: boolean;
    hasSubscriptionLimits: boolean;
    subscriptionsClusterUsage: number;
    subscriptionsDatabaseCount: number;
}

export const Default: StoryObj<AddNewOngoingTaskStoryArgs> = {
    name: "Add New Ongoing Task",
};

export const CloudProductionFeaturesNotTurnedOn: StoryObj<AddNewOngoingTaskStoryArgs> = {
    args: {
        databaseType: "singleNode",
        licenseType: "Enterprise",
        isCloud: true,
        hasOptionalFeatures: false,
    },
};

const OPTIONAL_FEATURES_OFF = {
    HasQueueEtl: false,
    HasQueueSink: false,
    HasOlapEtl: false,
    HasSnowflakeEtl: false,
    HasGenAi: false,
    HasAiAgent: false,
    HasEmbeddingsGeneration: false,
    HasRemoteAttachments: false,
    HasCdcSink: false,
} satisfies Partial<LicenseStatus>;

const commonInit = ({
    databaseType,
    licenseType,
    isCloud,
    hasOptionalFeatures,
    databaseAccess,
    hasSubscriptionLimits,
    subscriptionsClusterUsage,
    subscriptionsDatabaseCount,
}: AddNewOngoingTaskStoryArgs) => {
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

    const licenseOverrides: Partial<LicenseStatus> = {
        Type: licenseType,
        IsCloud: isCloud,
        ...(hasOptionalFeatures ? {} : OPTIONAL_FEATURES_OFF),
    };

    if (hasSubscriptionLimits) {
        license.with_LicenseLimited(licenseOverrides);
    } else {
        license.with_License(licenseOverrides);
    }

    license.with_LimitsUsage({
        NumberOfSubscriptionsInCluster: subscriptionsClusterUsage,
    });

    accessManager.with_securityClearance("ValidUser");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = subscriptionsDatabaseCount;
        dto.OngoingTasks = Array.from({ length: subscriptionsDatabaseCount }, (_, i) => ({
            ...TasksStubs.getSubscription(),
            TaskId: i + 1,
            SubscriptionId: i + 1,
            TaskName: `Subscription-${i + 1}`,
            SubscriptionName: `Subscription-${i + 1}`,
        }));
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
