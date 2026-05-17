import { AboutViewHeading } from "components/common/AboutView";
import React, { useCallback, useState } from "react";
import { Icon } from "components/common/Icon";
import "./TasksErrorsPage.scss";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useDatabaseWideAsync } from "hooks/useDatabaseWideAsync";
import { LoadingView } from "components/common/LoadingView";
import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import {
    AI_ONLY_TASK_TYPES,
    EtlTaskWithErrors,
    getTaskPillColor,
    getTasksWithErrors,
    GroupByType,
} from "./utils/tasksErrorsUtils";
import TasksErrorsAboutView from "./partials/TasksErrorsAboutView";
import { TaskPill, TaskPillGroupMessage } from "./partials/TaskPill";
import { TasksFilters, useTasksFilters } from "./partials/TasksFilters";
import { GroupByTaskView } from "./partials/GroupByTaskView";
import { GroupByNoneView } from "./partials/GroupByNoneView";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import appUrl from "common/appUrl";
import { ThemeColor } from "components/models/common";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

interface TasksErrorsPageQueryParams {
    taskName?: string;
    nodeTags?: string;
    shardNumbers?: string;
    healthStatuses?: string;
    taskTypes?: string;
    groupBy?: string;
}

interface TasksErrorsPageProps {
    aiOnly?: boolean;
}

export default function TasksErrorsPage({
    queryParams,
    aiOnly,
}: ReactQueryParamsProps<TasksErrorsPageQueryParams> & TasksErrorsPageProps) {
    const { isLoading, hasAnyError, handleRefresh, tasksWithErrors, flattenAllEtlStats } = useTasksErrorsData();

    if (isLoading) {
        return <LoadingView />;
    }

    if (hasAnyError) {
        return <LoadError refresh={handleRefresh} />;
    }

    return (
        <div className="content-padding tasks-errors-page">
            <div className="d-flex flex-column gap-2 flex-shrink-0">
                <div className="d-flex justify-content-between">
                    <AboutViewHeading marginBottom={0} title="Task Errors" icon="tasks-errors" />
                    <TasksErrorsAboutView />
                </div>
                {tasksWithErrors.length > 0 && <div>Analyze and get more details about your task errors.</div>}
            </div>
            <TasksErrorsPageBody
                tasksWithErrors={tasksWithErrors}
                flattenAllEtlStats={flattenAllEtlStats}
                initialSearchText={queryParams?.taskName}
                initialTaskTypes={
                    aiOnly
                        ? AI_ONLY_TASK_TYPES
                        : (queryParams?.taskTypes?.split(",").filter(Boolean) as StudioEtlType[])
                }
                initialNodeTags={queryParams?.nodeTags?.split(",").filter(Boolean)}
                initialShardNumbers={queryParams?.shardNumbers?.split(",").filter(Boolean)}
                initialHealthStatuses={
                    queryParams?.healthStatuses
                        ?.split(",")
                        .filter(Boolean) as Raven.Server.Documents.ETL.EtlProcessHealthStatus[]
                }
                initialGroupBy={queryParams?.groupBy as GroupByType}
                onRefresh={handleRefresh}
            />
        </div>
    );
}

interface TasksErrorsPageBodyProps {
    tasksWithErrors: EtlTaskWithErrors[];
    flattenAllEtlStats: EtlTaskStats[];
    initialSearchText?: string;
    initialTaskTypes?: StudioEtlType[];
    initialNodeTags?: string[];
    initialShardNumbers?: string[];
    initialHealthStatuses?: Raven.Server.Documents.ETL.EtlProcessHealthStatus[];
    initialGroupBy?: GroupByType;
    onRefresh: () => void;
}

const pillGroupOrder: Array<`bg-${ThemeColor}`> = ["bg-success", "bg-warning", "bg-danger"];

function getPillGroups(etlStats: EtlTaskStats[]) {
    return pillGroupOrder
        .map((color) => ({ color, stats: etlStats.filter((etl) => getTaskPillColor(etl.Stats) === color) }))
        .filter((group) => group.stats.length > 0);
}

function TasksErrorsPageBody({
    tasksWithErrors,
    flattenAllEtlStats,
    initialSearchText,
    initialTaskTypes,
    initialNodeTags,
    initialShardNumbers,
    initialHealthStatuses,
    initialGroupBy,
    onRefresh,
}: TasksErrorsPageBodyProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [selectedGroupByType, setSelectedGroupByType] = useState<GroupByType>(initialGroupBy ?? "task");

    const [filters, updateFilters] = useTasksFilters(
        (f) =>
            appUrl.forTasksErrors(databaseName, {
                taskName: f.searchText || undefined,
                nodeTags: f.nodeTags,
                shardNumbers: f.shardNumbers,
                healthStatuses: f.healthStatuses,
                taskTypes: f.taskTypes,
                groupBy: selectedGroupByType !== "task" ? selectedGroupByType : undefined,
            }),
        initialSearchText,
        initialTaskTypes,
        initialNodeTags,
        initialShardNumbers,
        initialHealthStatuses,
        [selectedGroupByType]
    );

    if (tasksWithErrors.length === 0) {
        return (
            <EmptySet>
                Your ongoing tasks are running smoothly. You can monitor and resolve any future task errors in this
                view.
            </EmptySet>
        );
    }

    return (
        <div className="d-flex flex-column flex-grow-1 min-h-0 mt-3">
            <div className="border-1 align-items-center d-flex w-100 bg-dark border-secondary border p-1 my-2 rounded flex-shrink-0">
                <Icon icon="tasks" />
                <span className="flex-grow">
                    <b>{tasksWithErrors.length ?? 0}</b> {tasksWithErrors.length === 1 ? "task" : "tasks"} with errors
                </span>
                <div className="d-flex gap-1 pills-container">
                    {getPillGroups(flattenAllEtlStats).map((group) => (
                        <PopoverWithHoverWrapper
                            key={group.color}
                            inline={false}
                            wrapperClassName="d-flex gap-1 pill-group"
                            message={
                                <TaskPillGroupMessage
                                    etlTaskStatsList={group.stats}
                                    allEtlTaskStats={flattenAllEtlStats}
                                    tasksWithErrors={tasksWithErrors}
                                />
                            }
                        >
                            {group.stats.map((etl, index) => (
                                <TaskPill color={group.color} key={etl.TaskName + index} />
                            ))}
                        </PopoverWithHoverWrapper>
                    ))}
                </div>
            </div>

            <TasksFilters
                selectedGroupByType={selectedGroupByType}
                setSelectedGroupByType={setSelectedGroupByType}
                filters={filters}
                updateFilters={updateFilters}
            />

            <div className="mt-4 mb-2 flex-grow-1 min-h-0">
                {selectedGroupByType === "task" && (
                    <GroupByTaskView
                        tasksWithErrors={tasksWithErrors}
                        etlStats={flattenAllEtlStats}
                        filters={filters}
                        onRefresh={onRefresh}
                    />
                )}
                {selectedGroupByType === "none" && (
                    <GroupByNoneView
                        tasksWithErrors={tasksWithErrors}
                        etlStats={flattenAllEtlStats}
                        filters={filters}
                        onRefresh={onRefresh}
                    />
                )}
            </div>
        </div>
    );
}

function useTasksErrorsData() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { tasksService } = useServices();

    const getEtlErrors = useCallback(
        async (location: databaseLocationSpecifier) => tasksService.getEtlErrors(db.name, location),
        [db.name]
    );

    const getEtlStats = useCallback(
        async (location: databaseLocationSpecifier) => tasksService.getEtlStats(db.name, location),
        [db.name]
    );

    const {
        result: asyncFetchAllEtlErrors,
        loading: isLoadingEtlErrors,
        refresh: refreshEtlErrors,
    } = useDatabaseWideAsync(getEtlErrors);

    const {
        result: asyncFetchAllEtlStats,
        loading: isLoadingEtlStats,
        refresh: refreshEtlStats,
    } = useDatabaseWideAsync(getEtlStats);

    const isLoading = isLoadingEtlErrors || isLoadingEtlStats;

    const hasAnyError =
        !isLoading && (asyncFetchAllEtlErrors.some((x) => x.error) || asyncFetchAllEtlStats.some((x) => x.error));

    const handleRefresh = useCallback(async () => {
        await refreshEtlErrors();
        await refreshEtlStats();
    }, [refreshEtlErrors, refreshEtlStats]);

    const flattenAllEtlStats: EtlTaskStats[] = isLoading ? [] : asyncFetchAllEtlStats.flatMap((x) => x.data);

    const tasksWithErrors = isLoading
        ? []
        : getTasksWithErrors(
              asyncFetchAllEtlErrors.flatMap((x) =>
                  (x.data ?? []).map((error) => ({
                      ...error,
                      nodeTag: x.location.nodeTag,
                      shardNumber: x.location.shardNumber,
                  }))
              ),
              flattenAllEtlStats
          );

    return { isLoading, hasAnyError, handleRefresh, tasksWithErrors, flattenAllEtlStats };
}
