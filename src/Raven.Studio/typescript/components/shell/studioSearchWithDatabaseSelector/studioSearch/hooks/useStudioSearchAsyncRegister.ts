import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import {
    StudioSearchItem,
    StudioSearchItemType,
    StudioSearchItemEvent,
    OngoingTaskWithBroker,
} from "../studioSearchTypes";
import { useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { useCallback, useMemo, useState } from "react";
import { useAsync } from "react-async-hook";

interface UseStudioSearchAsyncRegisterProps {
    register: (type: StudioSearchItemType, newItems: StudioSearchItem[]) => void;
    goToUrl: (url: string, newTab: boolean) => void;
    searchQuery: string;
}

export function useStudioSearchAsyncRegister(props: UseStudioSearchAsyncRegisterProps) {
    const { register, goToUrl, searchQuery } = props;

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const location = useMemo(
        () => (activeDatabase ? DatabaseUtils.getFirstLocation(activeDatabase, localNodeTag) : null),
        [activeDatabase, localNodeTag]
    );

    const { databasesService, indexesService, tasksService } = useServices();
    const { appUrl } = useAppUrls();

    const goToDocument = useCallback(
        (documentName: string, event: StudioSearchItemEvent) => {
            const url = appUrl.forEditDoc(documentName, activeDatabaseName);
            goToUrl(url, event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    const goToIndex = useCallback(
        (indexName: string, event: StudioSearchItemEvent) => {
            const url = appUrl.forEditIndex(indexName, activeDatabaseName);
            goToUrl(url, event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    const goToTask = useCallback(
        (
            taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
            brokerType: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType,
            taskId: number,
            event: StudioSearchItemEvent
        ) => {
            const getUrlFromProvider = (provider: (db: string, taskId?: number) => string) => {
                return provider(activeDatabaseName, taskId);
            };

            const getUrl = () => {
                switch (taskType) {
                    case "ElasticSearchEtl":
                        return getUrlFromProvider(appUrl.forEditElasticSearchEtl);
                    case "SqlEtl":
                        return getUrlFromProvider(appUrl.forEditSqlEtl);
                    case "SnowflakeEtl":
                        return getUrlFromProvider(appUrl.forEditSnowflakeEtl);
                    case "RavenEtl":
                        return getUrlFromProvider(appUrl.forEditRavenEtl);
                    case "Subscription":
                        return getUrlFromProvider(appUrl.forEditSubscription);
                    case "Replication":
                        return getUrlFromProvider(appUrl.forEditExternalReplication);
                    case "PullReplicationAsSink":
                        return getUrlFromProvider(appUrl.forEditReplicationSink);
                    case "PullReplicationAsHub":
                        return getUrlFromProvider(appUrl.forEditReplicationHub);
                    case "OlapEtl":
                        return getUrlFromProvider(appUrl.forEditOlapEtl);
                    case "Backup":
                        return appUrl.forEditPeriodicBackupTask("Backups", "OngoingTasks", false, taskId);
                    case "QueueEtl": {
                        if (brokerType === "Kafka") {
                            return getUrlFromProvider(appUrl.forEditKafkaEtl);
                        } else if (brokerType === "RabbitMq") {
                            return getUrlFromProvider(appUrl.forEditRabbitMqEtl);
                        } else {
                            return null;
                        }
                    }
                    case "QueueSink": {
                        if (brokerType === "Kafka") {
                            return getUrlFromProvider(appUrl.forEditKafkaSink);
                        } else if (brokerType === "RabbitMq") {
                            return getUrlFromProvider(appUrl.forEditRabbitMqSink);
                        } else {
                            return null;
                        }
                    }
                    default:
                        assertUnreachable(taskType);
                }
            };

            goToUrl(getUrl(), event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    const goToReplication = useCallback(
        (
            replicationMode: Raven.Client.Documents.Operations.Replication.PullReplicationMode,
            id: number,
            event: StudioSearchItemEvent
        ) => {
            let url = null;

            if (replicationMode === "HubToSink") {
                url = appUrl.forEditReplicationHub(activeDatabaseName, id);
            }
            if (replicationMode === "SinkToHub") {
                url = appUrl.forEditReplicationSink(activeDatabaseName, id);
            }

            goToUrl(url, event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    // Register tasks
    useAsync(
        async () => {
            if (!activeDatabaseName) {
                return {
                    OngoingTasks: [],
                    PullReplications: [],
                };
            }
            return tasksService.getOngoingTasks(activeDatabaseName, location);
        },
        [activeDatabaseName, location],
        {
            onSuccess(results: Raven.Server.Web.System.OngoingTasksResult) {
                const ongoingTasks: StudioSearchItem[] = (results.OngoingTasks as OngoingTaskWithBroker[]).map((x) => ({
                    type: "task",
                    icon: "ongoing-tasks",
                    text: x.TaskName,
                    onSelected: (e) => goToTask(x.TaskType, x.BrokerType, x.TaskId, e),
                }));

                const pullReplications: StudioSearchItem[] = results.PullReplications.map((x) => ({
                    type: "task",
                    icon: "replication",
                    text: x.Name,
                    onSelected: (e) => goToReplication(x.Mode, x.TaskId, e),
                }));

                register("task", [...ongoingTasks, ...pullReplications]);
            },
            onError() {
                register("task", []);
            },
        }
    );

    // Register indexes
    useAsync(
        async () => {
            if (!activeDatabaseName) {
                return [];
            }
            return indexesService.getStats(activeDatabaseName, location);
        },
        [activeDatabaseName, location],
        {
            onSuccess(results) {
                register(
                    "index",
                    results.map((x) => ({
                        type: "index",
                        icon: "index",
                        text: x.Name,
                        onSelected: (e) => goToIndex(x.Name, e),
                    }))
                );
            },
            onError() {
                register("index", []);
            },
        }
    );

    const [lastDocumentsCount, setLastDocumentsCount] = useState(0);

    // Register documents
    useAsyncDebounce(
        async (searchQuery, activeDatabaseName) => {
            if (!activeDatabaseName || !searchQuery) {
                return [];
            }

            return databasesService.getDocumentsMetadataByIDPrefix(searchQuery, 10, activeDatabaseName);
        },
        [searchQuery, activeDatabaseName],
        500,
        {
            onSuccess: (results) => {
                // When previous and current results are empty do nothing
                if (results?.length === 0 && lastDocumentsCount === 0) {
                    return;
                }

                setLastDocumentsCount(results?.length ?? 0);

                const mappedResults = results.map((x) => x["@metadata"]["@id"]);

                register(
                    "document",
                    mappedResults.map((result) => ({
                        type: "document",
                        icon: "document",
                        text: result,
                        onSelected: (e) => goToDocument(result, e),
                        subText: null,
                    }))
                );
            },
            onError() {
                register("document", []);
            },
        }
    );
}
