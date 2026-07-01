import React, { useMemo } from "react";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import Card from "react-bootstrap/Card";
import Collapse from "react-bootstrap/Collapse";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import SizeGetter from "components/common/SizeGetter";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "components/common/RichPanel";
import useBoolean from "hooks/useBoolean";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    EtlHealthStatus,
    EtlTaskWithErrors,
    EtlTransformationWithErrors,
    FlatError,
    flattenTransformationErrors,
    getEtlEditLink,
    getEtlTypeIcon,
    getEtlTypeLabel,
    getPopoverMessageForTaskHealth,
    getTaskCategory,
    getTaskHealthStatus,
    healthStatusToBadge,
    SHOW_WIDTH_SIZE,
} from "../utils/tasksErrorsUtils";
import {
    CellAffectedDocumentsWrapper,
    CellDateWithRelativeTimeWrapper,
    CellErrorStepWrapper,
    CellErrorTypeWrapper,
    CellNodeValueWrapper,
    CellShardValueWrapper,
    CellValueButtonWrapper,
    HyperLinkDocumentCellValue,
} from "./TasksErrorsCells";
import { DeleteErrorsModal } from "./DeleteModals";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AccessPopover } from "components/common/AccessPopover";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

interface EtlTypeRichPanelItemProps {
    etlType: StudioEtlType;
}

function EtlTypeRichPanelItem({ etlType }: EtlTypeRichPanelItemProps) {
    const icon = getEtlTypeIcon(etlType);
    const label = getEtlTypeLabel(etlType);

    return (
        <RichPanelDetailItem>
            <Icon icon={icon} />
            <span>{label}</span>
        </RichPanelDetailItem>
    );
}

function useTasksErrorsPanelTableColumns(availableWidth: number, hasProcessErrors: boolean) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth - SHOW_WIDTH_SIZE), [bodyWidth]);

    const tasksErrorsPanelColumns: ColumnDef<FlatError>[] = useMemo(
        () => [
            {
                header: "Show",
                cell: CellValueButtonWrapper,
                size: SHOW_WIDTH_SIZE,
            },
            {
                header: "Error type",
                accessorKey: "errorType",
                cell: CellErrorTypeWrapper,
                size: getSize(10),
            },
            {
                header: "Error step",
                cell: CellErrorStepWrapper,
                accessorKey: "Step",
                size: getSize(10),
            },
            {
                header: "Document",
                cell: HyperLinkDocumentCellValue,
                accessorKey: "DocumentId",
                size: getSize(10),
            },
            {
                header: "Date",
                cell: CellDateWithRelativeTimeWrapper,
                accessorKey: "CreatedAt",
                size: getSize(20),
            },
            ...(hasProcessErrors
                ? [
                      {
                          header: "Affected Documents",
                          cell: CellAffectedDocumentsWrapper,
                          accessorKey: "AffectedDocumentsCount",
                          size: getSize(10),
                      },
                  ]
                : []),
            {
                header: "Error",
                cell: CellWithCopyWrapper,
                accessorKey: "Error",
                size: getSize((db.isSharded ? 40 : 45) - (hasProcessErrors ? 10 : 0)),
                enableSorting: false,
            },
            {
                header: "Node",
                cell: CellNodeValueWrapper,
                accessorKey: "nodeTag",
                size: getSize(3),
                enableSorting: false,
                enableColumnFilter: false,
            },
        ],
        [getSize, hasProcessErrors]
    );

    if (db.isSharded) {
        tasksErrorsPanelColumns.push({
            header: "Shard",
            cell: CellShardValueWrapper,
            accessorKey: "shardNumber",
            size: getSize(3),
            enableSorting: false,
        });
    }

    return tasksErrorsPanelColumns;
}

interface NestedTaskPanelDetailsTableProps {
    width: number;
    itemErrors: EtlTransformationWithErrors["itemErrors"];
    processErrors: EtlTransformationWithErrors["processErrors"];
    etlName: string;
    transformationName: string;
    healthStatus: EtlHealthStatus;
    taskId?: number;
    etlType?: StudioEtlType;
}

function NestedTaskPanelDetailsTable({
    width,
    itemErrors,
    processErrors,
    etlName,
    transformationName,
    healthStatus,
    taskId,
    etlType,
}: NestedTaskPanelDetailsTableProps) {
    const columns = useTasksErrorsPanelTableColumns(width, processErrors.length > 0);
    const data = useMemo<FlatError[]>(
        () =>
            flattenTransformationErrors(itemErrors, processErrors).map((e) => ({
                ...e,
                etlName,
                transformationName,
                healthStatus,
                taskId,
                etlType,
            })),
        [itemErrors, processErrors, etlName, transformationName, healthStatus, taskId, etlType]
    );

    const tasksErrorsPanelTable = useReactTable({
        data,
        columns,
        columnResizeMode: "onChange",
        initialState: {
            sorting: [{ id: "CreatedAt", desc: true }],
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <VirtualTable
            table={tasksErrorsPanelTable}
            heightInPx={virtualTableUtils.getHeightInPx(
                data.length,
                virtualTableConstants.defaultTableHeightInPx,
                virtualTableConstants.doubleLineRowHeightInPx
            )}
            rowHeightInPx={virtualTableConstants.doubleLineRowHeightInPx}
        />
    );
}

interface NestedTaskPanelDetailsProps extends EtlTransformationWithErrors {
    width: number;
    etlName: string;
    healthStatus: EtlHealthStatus;
    taskId?: number;
    etlType?: StudioEtlType;
}

function NestedTaskPanelDetails({
    width,
    transformationName,
    processErrors,
    itemErrors,
    ...rest
}: NestedTaskPanelDetailsProps) {
    const { value: isNestedDetailsVisible, toggle: toggleNestedDetailsVisible } = useBoolean(true);

    const totalErrors = processErrors.length + itemErrors.length;
    const isAiTask = getTaskCategory(rest.etlType) === "Ai";

    if (isAiTask) {
        return (
            <Card className="bg-black p-3">
                <NestedTaskPanelDetailsTable
                    width={width}
                    itemErrors={itemErrors}
                    processErrors={processErrors}
                    transformationName={transformationName}
                    {...rest}
                />
            </Card>
        );
    }

    return (
        <Card className="bg-black p-3">
            <div className="d-flex w-100 gap-2 align-items-center">
                <span className="h4 mb-0">{transformationName}</span>
                <div className="flex-grow">
                    <Icon icon="warning" color="danger" />
                    <span>Errors</span> <b>{totalErrors}</b>
                </div>
                <Button variant="secondary" onClick={toggleNestedDetailsVisible}>
                    <Icon icon={isNestedDetailsVisible ? "collapse-vertical" : "expand-vertical"} margin="m-0" />
                </Button>
            </div>
            <Collapse in={isNestedDetailsVisible} unmountOnExit mountOnEnter>
                <div className="mt-3">
                    <NestedTaskPanelDetailsTable
                        width={width}
                        itemErrors={itemErrors}
                        processErrors={processErrors}
                        transformationName={transformationName}
                        {...rest}
                    />
                </div>
            </Collapse>
        </Card>
    );
}

interface TaskPanelProps extends EtlTaskWithErrors {
    etlStats: EtlTaskStats[];
    onRefresh: () => void;
}

export function TaskPanel({ etlName, etlType, transformations, etlStats, onRefresh }: TaskPanelProps) {
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { value: isDetailsVisible, toggle: toggleDetails } = useBoolean(true);
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);

    const errorsCount = transformations.reduce(
        (acc, transformation) => acc + transformation.processErrors.length + transformation.itemErrors.length,
        0
    );

    const taskHealth = getTaskHealthStatus(etlStats, etlName);
    const { bg, icon, label } = healthStatusToBadge(taskHealth);
    const taskStats = etlStats.find((s) => s.TaskName === etlName);
    const taskId = taskStats?.TaskId;

    const taskLink = getEtlEditLink(databaseName, taskId, etlType);

    return (
        <>
            <RichPanel>
                <RichPanelHeader>
                    <RichPanelInfo>
                        <a href={taskLink ?? "#"} className="fs-3">
                            {etlName}
                        </a>
                    </RichPanelInfo>
                    <RichPanelActions>
                        <AccessPopover accessRequired="DatabaseReadWrite">
                            <Button variant="danger" disabled={!hasDatabaseWriteAccess} onClick={toggleDeleteModal}>
                                <Icon icon="trash" />
                                Delete errors
                            </Button>
                        </AccessPopover>
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem>
                        <Button
                            variant="secondary"
                            className="btn-toggle-panel rounded-pill"
                            onClick={toggleDetails}
                            title="Click for details"
                        >
                            <Icon icon={isDetailsVisible ? "fold" : "unfold"} margin="m-0" />
                        </Button>
                    </RichPanelDetailItem>
                    {etlType && <EtlTypeRichPanelItem etlType={etlType} />}
                    <RichPanelDetailItem contentClassName="d-flex gap-1 align-items-center">
                        <Icon icon="warning" color="danger" margin="m-0" />
                        <span>Errors</span> <b>{errorsCount}</b>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem contentClassName="d-flex gap-1 align-items-center">
                        <Icon icon="console" />
                        <span>Scripts</span> <b>{transformations.length}</b>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem>
                        <PopoverWithHoverWrapper
                            wrapperClassName="d-flex align-items-center"
                            message={getPopoverMessageForTaskHealth(taskHealth)}
                        >
                            <Icon icon="healthcheck" />
                            <Badge bg={bg} className="rounded-pill">
                                <Icon icon={icon} />
                                {label}
                            </Badge>
                        </PopoverWithHoverWrapper>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem>
                        <PopoverWithHoverWrapper
                            wrapperClassName="d-flex gap-1 align-items-center"
                            message="The node this task is currently running on"
                        >
                            <Icon icon="node" />
                            <span>{taskStats?.NodeTag ?? "-"}</span>
                        </PopoverWithHoverWrapper>
                    </RichPanelDetailItem>
                    {db.isSharded && (
                        <RichPanelDetailItem>
                            <PopoverWithHoverWrapper
                                wrapperClassName="d-flex gap-1 align-items-center"
                                message="The shard this task is currently running on"
                            >
                                <Icon icon="shard" color="shard" />
                                <span>{taskStats?.ShardNumber ?? "-"}</span>
                            </PopoverWithHoverWrapper>
                        </RichPanelDetailItem>
                    )}
                </RichPanelDetails>
                <SizeGetter
                    render={(sizeProps) => (
                        <Collapse in={isDetailsVisible} unmountOnExit mountOnEnter>
                            <div className="m-2 d-flex gap-2 flex-column">
                                {transformations.map((transformation) => (
                                    <NestedTaskPanelDetails
                                        key={transformation.transformationName}
                                        {...sizeProps}
                                        {...transformation}
                                        etlName={etlName}
                                        healthStatus={taskHealth}
                                        taskId={taskId}
                                        etlType={etlType}
                                    />
                                ))}
                            </div>
                        </Collapse>
                    )}
                />
            </RichPanel>
            {isDeleteModalOpen && (
                <DeleteErrorsModal
                    mode="task"
                    etlName={etlName}
                    etlType={etlType}
                    transformations={transformations}
                    errorsCount={errorsCount}
                    toggle={toggleDeleteModal}
                    onRefresh={onRefresh}
                />
            )}
        </>
    );
}
