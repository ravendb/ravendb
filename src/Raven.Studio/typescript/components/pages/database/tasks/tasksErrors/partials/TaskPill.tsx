import React from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import { EtlTaskWithErrors, getHealthStatusFromStats, healthStatusToBadge } from "../utils/tasksErrorsUtils";
import { ThemeColor } from "components/models/common";
import { databaseLocationComparator } from "components/utils/common";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

export interface TaskPillProps {
    color: `bg-${ThemeColor}`;
}

export function TaskPill({ color }: TaskPillProps) {
    return <div className={classNames("tasks-pill rounded", color)} />;
}

interface TaskPillGroupMessageProps {
    etlTaskStatsList: EtlTaskStats[];
    allEtlTaskStats: EtlTaskStats[];
    tasksWithErrors: EtlTaskWithErrors[];
}

export function TaskPillGroupMessage({
    etlTaskStatsList,
    allEtlTaskStats,
    tasksWithErrors,
}: TaskPillGroupMessageProps) {
    const overallHealth = getHealthStatusFromStats(etlTaskStatsList[0].Stats);
    const { bg, icon, label } = healthStatusToBadge(overallHealth);

    const taskNames: string[] = _.uniq(etlTaskStatsList.map((s) => s.TaskName));

    const rows = taskNames.flatMap((taskName: string): TaskPillRowProps[] => {
        const taskWithErrors = tasksWithErrors.find((t) => t.etlName === taskName);
        const allErrors = taskWithErrors?.transformations.flatMap((t) => [...t.itemErrors, ...t.processErrors]) ?? [];

        const ownLocations: databaseLocationSpecifier[] = etlTaskStatsList
            .filter((s) => s.TaskName === taskName)
            .map((s) => ({ nodeTag: s.NodeTag, shardNumber: s.ShardNumber }));

        const taskCurrentLocations: databaseLocationSpecifier[] = allEtlTaskStats
            .filter((s) => s.TaskName === taskName)
            .map((s) => ({ nodeTag: s.NodeTag, shardNumber: s.ShardNumber }));

        const orphanLocations = allErrors
            .map((e): databaseLocationSpecifier => ({ nodeTag: e.nodeTag, shardNumber: e.shardNumber }))
            .filter((loc) => !taskCurrentLocations.some((current) => databaseLocationComparator(current, loc)));

        const locations = _.uniqWith([...ownLocations, ...orphanLocations], databaseLocationComparator);

        return locations.map((loc: databaseLocationSpecifier) => ({
            taskName,
            nodeTag: loc.nodeTag,
            shardNumber: loc.shardNumber,
            errorCount: allErrors.filter((e) => databaseLocationComparator(e, loc)).length,
        }));
    });

    return (
        <div className="task-pill-message">
            <div className="d-flex align-items-center gap-2 mb-2">
                <div>
                    <b className="flex-grow text-nowrap">{etlTaskStatsList.length}</b>{" "}
                    {etlTaskStatsList.length === 1 ? "task" : "tasks"}
                </div>
                <Badge bg={bg} className="rounded-pill">
                    <Icon icon={icon} />
                    {label}
                </Badge>
            </div>
            <div className="d-flex flex-column gap-1">
                {rows.map((row, index) => (
                    <TaskPillRow key={row.taskName + index} {...row} />
                ))}
            </div>
        </div>
    );
}

interface TaskPillRowProps extends databaseLocationSpecifier {
    taskName: string;
    errorCount: number;
}

function TaskPillRow({ nodeTag, shardNumber, taskName, errorCount }: TaskPillRowProps) {
    return (
        <div className="d-flex align-items-center gap-2">
            <div className="d-flex flex-grow-1 gap-1">
                <span className="text-truncate small">{taskName}</span>
                {nodeTag && (
                    <span className="text-nowrap small flex-shrink-0">
                        <Icon icon="node" color="node" />
                        <span>{nodeTag}</span>
                    </span>
                )}
                {shardNumber != null && (
                    <span className="text-nowrap small flex-shrink-0">
                        <Icon icon="shard" color="shard" />
                        <span>#{shardNumber}</span>
                    </span>
                )}
            </div>
            <span className="small text-muted ms-auto text-nowrap flex-shrink-0">
                <Icon icon="warning" color="danger" margin="m-0" />
                <b> {errorCount}</b> {errorCount === 1 ? "error" : "errors"}
            </span>
        </div>
    );
}
