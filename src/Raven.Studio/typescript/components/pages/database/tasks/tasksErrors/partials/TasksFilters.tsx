import React, { useMemo } from "react";
import { Icon } from "components/common/Icon";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Select, { SelectOption } from "components/common/select/Select";
import { InputItem } from "components/models/common";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { EtlHealthStatus, GroupByType, TasksFiltersState } from "../utils/tasksErrorsUtils";
import { useUrlFilters } from "components/hooks/useUrlFilters";

export type { TasksFiltersState };

export const taskHealthOptions: SelectOption<EtlHealthStatus>[] = [
    { label: "Healthy", value: "Healthy" },
    { label: "Failed", value: "Failed" },
    { label: "Impaired", value: "Impaired" },
];

export const taskTypeOptions: SelectOption<StudioTaskType>[] = [
    { label: "RavenDB ETL", value: "RavenEtl" },
    { label: "SQL ETL", value: "SqlEtl" },
    { label: "Snowflake ETL", value: "SnowflakeEtl" },
    { label: "Azure Queue Storage ETL", value: "AzureQueueStorageQueueEtl" },
    { label: "OLAP ETL", value: "OlapEtl" },
    { label: "Kafka ETL", value: "KafkaQueueEtl" },
    { label: "Elastic Search ETL", value: "ElasticSearchEtl" },
    { label: "RabbitMQ ETL", value: "RabbitQueueEtl" },
    { label: "Amazon SQS ETL", value: "AmazonSqsQueueEtl" },
    { label: "Embeddings Generation", value: "EmbeddingsGeneration" },
    { label: "GenAI", value: "GenAi" },
    { label: "CDC Sink", value: "CdcSink" },
];

export const groupByOptions: InputItem<GroupByType>[] = [
    { value: "task", label: "Task", badgeColor: "secondary" },
    { value: "none", label: "None", badgeColor: "secondary" },
];

export function useTasksFilters(
    buildUrl: (filters: TasksFiltersState) => string,
    initialSearchText = "",
    initialTaskTypes: StudioTaskType[] = [],
    initialNodeTags: string[] = [],
    initialShardNumbers: string[] = [],
    initialHealthStatuses: EtlHealthStatus[] = [],
    extraDeps: unknown[] = []
): [TasksFiltersState, (patch: Partial<TasksFiltersState>) => void] {
    return useUrlFilters<TasksFiltersState>(
        {
            searchText: initialSearchText,
            nodeTags: initialNodeTags,
            shardNumbers: initialShardNumbers,
            healthStatuses: initialHealthStatuses,
            taskTypes: initialTaskTypes,
        },
        buildUrl,
        extraDeps
    );
}

interface TasksFiltersProps {
    selectedGroupByType: GroupByType;
    setSelectedGroupByType: (x: GroupByType) => void;
    filters: TasksFiltersState;
    updateFilters: (patch: Partial<TasksFiltersState>) => void;
}

export function TasksFilters({
    setSelectedGroupByType,
    selectedGroupByType,
    filters,
    updateFilters,
}: TasksFiltersProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const nodeOptions = useMemo(() => {
        const tags: string[] = db.isSharded
            ? _.uniq(DatabaseUtils.getLocations(db).map((l) => l.nodeTag)).sort()
            : db.nodes.map((node) => node.tag);

        return tags.map((tag) => ({ label: `Node ${tag}`, value: tag }));
    }, [db]);

    const shardOptions = useMemo(() => {
        if (!db.isSharded) {
            return [];
        }
        const shardNumbers: number[] = _.uniq(
            DatabaseUtils.getLocations(db)
                .map((l) => l.shardNumber)
                .filter((n) => n != null)
        );

        return shardNumbers.map((n) => ({ label: `Shard #${n}`, value: String(n) }));
    }, [db]);

    return (
        <div className="hstack flex-wrap align-items-end gap-2 my-3 justify-content-start">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">Filter by task/script name</div>
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder="e.g. MyPeriodicBackupTask"
                        title="Filter ongoing tasks"
                        className="filtering-input"
                        value={filters.searchText}
                        onChange={(e) => updateFilters({ searchText: e.target.value })}
                    />
                    {filters.searchText && (
                        <div className="clear-button">
                            <Button variant="secondary" size="sm" onClick={() => updateFilters({ searchText: "" })}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">Filter by node</div>
                <Select
                    isMulti
                    isClearable
                    options={nodeOptions}
                    value={nodeOptions.filter((o) => filters.nodeTags.includes(o.value))}
                    onChange={(options) => updateFilters({ nodeTags: options ? options.map((o) => o.value) : [] })}
                />
            </div>
            {db.isSharded && (
                <div className="TaskErrorFilter">
                    <div className="small-label ms-1 mb-1">Filter by shard</div>
                    <Select
                        isMulti
                        isClearable
                        options={shardOptions}
                        value={shardOptions.filter((o) => filters.shardNumbers.includes(o.value))}
                        onChange={(options) =>
                            updateFilters({ shardNumbers: options ? options.map((o) => o.value) : [] })
                        }
                    />
                </div>
            )}
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">Filter by task type</div>
                <Select
                    isMulti
                    isClearable
                    options={taskTypeOptions}
                    value={taskTypeOptions.filter((o) => filters.taskTypes.includes(o.value))}
                    onChange={(options) =>
                        updateFilters({
                            taskTypes: options ? options.map((o) => o.value) : [],
                        })
                    }
                />
            </div>
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">Filter by task health</div>
                <Select
                    isMulti
                    isClearable
                    options={taskHealthOptions}
                    value={taskHealthOptions.filter((o) => filters.healthStatuses.includes(o.value))}
                    onChange={(options) =>
                        updateFilters({
                            healthStatuses: options ? options.map((o) => o.value) : [],
                        })
                    }
                />
            </div>
            <div>
                <div className="small-label ms-1 mb-1">Group by</div>
                <MultiRadioToggle<GroupByType>
                    inputItems={groupByOptions}
                    selectedItem={selectedGroupByType}
                    setSelectedItem={(x) => setSelectedGroupByType(x)}
                />
            </div>
        </div>
    );
}
