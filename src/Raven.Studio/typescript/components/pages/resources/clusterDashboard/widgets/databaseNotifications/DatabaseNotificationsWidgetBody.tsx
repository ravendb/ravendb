import {
    ColumnDef,
    ColumnFiltersState,
    ExpandedState,
    getCoreRowModel,
    getExpandedRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useResizeObserver } from "components/hooks/useResizeObserver";
import databaseNotificationsItem from "models/resources/widgets/databaseNotificationsItem";
import { groupBy, sumBy } from "lodash";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Button from "react-bootstrap/Button";
import DatabaseUtils from "components/utils/DatabaseUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import databasesManager from "common/shell/databasesManager";
import notificationCenter from "common/notifications/notificationCenter";
import { useAppUrls } from "components/hooks/useAppUrls";

import "./DatabaseNotificationsWidgetBody.scss";

interface DatabaseNotificationsWidgetBodyProps {
    flatItems: databaseNotificationsItem[];
    columnFilters: ColumnFiltersState;
    setColumnFilters: (state: ColumnFiltersState) => void;
}

export default function DatabaseNotificationsWidgetBody({
    flatItems,
    columnFilters,
    setColumnFilters,
}: DatabaseNotificationsWidgetBodyProps) {
    const ref = useRef<HTMLDivElement>(null);
    const { width } = useResizeObserver({ ref });

    const summary = useMemo(() => calculateSummary(flatItems), [flatItems]);
    const rowItems = useMemo(() => mapToRowItems(flatItems), [flatItems]);
    const columns = useColumns(width);
    const [expanded, setExpanded] = useState<ExpandedState>({});

    const table = useReactTable({
        data: rowItems,
        columns,
        state: {
            expanded,
            columnFilters,
        },
        filterFromLeafRows: true,
        getSubRows: (row) => row.subRows,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getRowCanExpand: (row) => row.original.nodeTag != null && row.original.subRows?.length > 0,
        getIsRowExpanded: (row) => {
            if (expanded === true) {
                return true;
            }
            return expanded[row.id] ?? false;
        },
        onColumnFiltersChange: (updater) => {
            const updaterValue = typeof updater === "function" ? updater(columnFilters) : updater;
            setColumnFilters(updaterValue);
        },
        onExpandedChange: (updater) => {
            const updaterValue = typeof updater === "function" ? updater(expanded) : updater;

            // Empty object means all are collapsed so it should expand only dbs with multi nodes
            if (typeof updaterValue === "object" && Object.keys(updaterValue).length === 0) {
                const multiNodesExpanded: Record<string, boolean> = {};
                rowItems.forEach((rowItem, index) => {
                    multiNodesExpanded[index] = rowItem.nodeTag == null;
                    rowItem.subRows?.forEach((_, subIndex) => {
                        multiNodesExpanded[`${index}.${subIndex}`] = false;
                    });
                });

                setExpanded(multiNodesExpanded);
                return;
            }

            setExpanded(updaterValue);
            return;
        },
    });

    // Set initial expanded state
    useEffect(() => {
        const initialExpanded: Record<string, boolean> = {};

        table.getRowModel().rows.forEach((row) => {
            const isMultiNode = row.original.nodeTag == null;
            initialExpanded[row.id] = row.getIsExpanded() || isMultiNode;

            row.subRows?.forEach((subRow) => {
                initialExpanded[subRow.id] = row.getIsExpanded();
            });
        });

        setExpanded(initialExpanded);
    }, [flatItems]);

    return (
        <div ref={ref} className="p-1 pt-0">
            <Summary summary={summary} />
            <VirtualTable table={table} heightInPx={300} isCompact isRoundingDisabled isPaddingDisabled />
            <div className="pt-1 font-size-11 text-center">
                <Icon icon="info" />
                This list only displays databases that are currently online. Offline databases will not appear.
            </div>
        </div>
    );
}

interface Summary {
    totalNotificationsCount: number;
    totalAlertsCount: number;
    totalPerfHintsCount: number;
}

function Summary(props: { summary: Summary }) {
    const { totalNotificationsCount, totalAlertsCount, totalPerfHintsCount } = props.summary;

    return (
        <div
            className="hstack gap-1 p-2 panel-bg-1 rounded-2 mb-2 flex-wrap justify-content-between"
            style={{ height: "37.27px" }}
        >
            <div className="small-label">
                Total <span>{totalNotificationsCount.toLocaleString()}</span>{" "}
                <Icon icon="notifications" margin="ms-1" />
            </div>
            <div className="hstack gap-3">
                <div className="small-label">
                    Alerts <span>{totalAlertsCount.toLocaleString()}</span>
                    <Icon icon="warning" margin="ms-1" color="warning" />
                </div>
                <div className="small-label">
                    Performance hints <span>{totalPerfHintsCount.toLocaleString()}</span>
                    <Icon icon="performance" margin="ms-1" color="info" />
                </div>
            </div>
        </div>
    );
}

function calculateSummary(items: databaseNotificationsItem[]): Summary {
    const totalAlertsCount = sumBy(items, (x) => x.alertsCount);
    const totalPerfHintsCount = sumBy(items, (x) => x.performanceHintsCount);
    const totalNotificationsCount = totalAlertsCount + totalPerfHintsCount;

    return {
        totalNotificationsCount,
        totalAlertsCount,
        totalPerfHintsCount,
    };
}

interface RowItem {
    dbName?: string;
    nodeTag?: string;

    alertsCount?: number;
    alertPrettifiedReason?: string;

    perfHintsCount?: number;
    perfHintPrettifiedReason?: string;

    subRows?: RowItem[];
}

export function mapToRowItems(flatItems: databaseNotificationsItem[]): RowItem[] {
    const byDatabase = groupBy(flatItems, (x) => x.database);

    return Object.entries(byDatabase)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([dbName, dbItems]): RowItem => {
            const isSingleNode = dbItems.length === 1;
            const isMultiNode = dbItems.length > 1;

            if (isSingleNode) {
                const item = dbItems[0];

                return {
                    dbName: item.database,
                    nodeTag: item.nodeTag,
                    alertsCount: item.alertsCount,
                    perfHintsCount: item.performanceHintsCount,
                    subRows: createSubRowsWithReason(item),
                };
            }

            if (isMultiNode) {
                return {
                    dbName: dbName,
                    alertsCount: sumBy(dbItems, (x) => x.alertsCount),
                    perfHintsCount: sumBy(dbItems, (x) => x.performanceHintsCount),
                    subRows: dbItems
                        .sort((a, b) => a.nodeTag.localeCompare(b.nodeTag))
                        .map((item) => ({
                            nodeTag: item.nodeTag,
                            alertsCount: item.alertsCount,
                            perfHintsCount: item.performanceHintsCount,
                            subRows: createSubRowsWithReason(item),
                        })),
                };
            }
        });
}

function createSubRowsWithReason(item: databaseNotificationsItem): RowItem[] {
    const alerts = item.alerts ?? [];
    const hints = item.performanceHints ?? [];
    const maxLength = Math.max(alerts.length, hints.length);

    return Array.from({ length: maxLength }, (_, i): RowItem => {
        const alert = alerts[i];
        const hint = hints[i];

        return {
            alertsCount: alert?.Count,
            alertPrettifiedReason: alert?.PrettifiedReason,
            perfHintsCount: hint?.Count,
            perfHintPrettifiedReason: hint?.PrettifiedReason,
        };
    });
}

function useColumns(availableWidthInPx: number): ColumnDef<RowItem>[] {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidthInPx);
    const getSize = useCallback(virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const { appUrl } = useAppUrls();

    const openNotificationCenterForDatabase = (databaseName: string, nodeTag: string) => {
        const db = databasesManager.default.getDatabaseByName(databaseName);
        if (!db) {
            throw new Error("Cannot find database: " + databaseName);
        }

        if (nodeTag === localNodeTag) {
            databasesManager.default.activate(db);
            notificationCenter.instance.showNotifications.toggle();
        } else {
            const serverUrl = allNodes.find((n) => n.nodeTag === nodeTag)?.serverUrl;
            if (!serverUrl) {
                throw new Error("Cannot find server URL for node: " + nodeTag);
            }

            const targetUrl = appUrl.toExternalUrl(serverUrl, appUrl.forClusterDashboard());
            window.open(targetUrl, "_blank");
        }
    };

    return useMemo(
        (): ColumnDef<RowItem>[] => [
            {
                header: "Database",
                accessorFn: (x) => DatabaseUtils.formatName(x.dbName),
                cell: ({ getValue }) => <span>{getValue<string>()}</span>,
                size: getSize(25),
            },
            {
                id: "Node",
                header: ({ table }) => {
                    return (
                        <div className="hstack justify-content-between">
                            <div>Node</div>
                            <Button
                                variant="link"
                                onClick={table.getToggleAllRowsExpandedHandler()}
                                size="xs"
                                className="p-0 link-muted font-size-11 line-height-25"
                                title="Expand all nodes"
                            >
                                <Icon
                                    icon={table.getIsAllRowsExpanded() ? "collapse-vertical" : "expand-vertical"}
                                    margin="m-0"
                                    className="d-flex align-items-center line-height-25"
                                />
                            </Button>
                        </div>
                    );
                },
                accessorFn: (x) => x.nodeTag,
                cell: ({ row, getValue }) => {
                    const value = getValue<string>();

                    const dbName = row.depth === 0 ? row.original.dbName : row.getParentRow()?.original.dbName;

                    return (
                        <div className="hstack justify-content-between w-100">
                            {value && (
                                <Button
                                    variant="link"
                                    onClick={() => openNotificationCenterForDatabase(dbName, value)}
                                    className="p-0"
                                    style={{ height: "18px" }}
                                >
                                    <span className={`node-label node-${value}`}>{value}</span>
                                </Button>
                            )}
                            {row.getCanExpand() && (
                                <Button
                                    variant="link"
                                    onClick={row.getToggleExpandedHandler()}
                                    size="xs"
                                    className="p-0 link-muted"
                                    title="Expand node"
                                >
                                    <Icon
                                        icon={row.getIsExpanded() ? "collapse-vertical" : "expand-vertical"}
                                        className="d-flex align-items-center line-height-25"
                                        margin="m-0"
                                    />
                                </Button>
                            )}
                        </div>
                    );
                },
                size: getSize(15),
                enableSorting: false,
                enableColumnFilter: false,
            },
            {
                header: "Alerts",
                accessorFn: ({ alertsCount, alertPrettifiedReason }) => ({ alertsCount, alertPrettifiedReason }),
                cell: ({ getValue }) => {
                    const value = getValue<Pick<RowItem, "alertsCount" | "alertPrettifiedReason">>();

                    return (
                        <div className="hstack justify-content-between gap-1">
                            {value.alertPrettifiedReason && (
                                <PopoverWithHoverWrapper message={value.alertPrettifiedReason}>
                                    {value.alertPrettifiedReason}
                                </PopoverWithHoverWrapper>
                            )}
                            {value.alertsCount > 0 && (
                                <div className="hstack flex-shrink-0 gap-1">
                                    <Icon icon="warning" margin="m-0" color="warning" />
                                    <span className="m-0">{value.alertsCount.toLocaleString()}</span>
                                </div>
                            )}
                        </div>
                    );
                },
                size: getSize(30),
                sortingFn: (rowA, rowB) => {
                    const a = rowA.original.alertsCount || 0;
                    const b = rowB.original.alertsCount || 0;
                    return a - b;
                },
            },
            {
                header: "Performance hints",
                accessorFn: ({ perfHintsCount, perfHintPrettifiedReason }) => ({
                    perfHintsCount,
                    perfHintPrettifiedReason,
                }),
                cell: ({ getValue }) => {
                    const value = getValue<Pick<RowItem, "perfHintsCount" | "perfHintPrettifiedReason">>();

                    return (
                        <div className="hstack justify-content-between gap-1">
                            {value.perfHintPrettifiedReason && (
                                <PopoverWithHoverWrapper message={value.perfHintPrettifiedReason}>
                                    {value.perfHintPrettifiedReason}
                                </PopoverWithHoverWrapper>
                            )}
                            {value.perfHintsCount > 0 && (
                                <div className="hstack flex-shrink-0 gap-1">
                                    <Icon icon="performance" margin="m-0" color="info" />
                                    <span className="m-0">{value.perfHintsCount.toLocaleString()}</span>
                                </div>
                            )}
                        </div>
                    );
                },
                size: getSize(30),
                sortingFn: (rowA, rowB) => {
                    const a = rowA.original.perfHintsCount || 0;
                    const b = rowB.original.perfHintsCount || 0;
                    return a - b;
                },
            },
        ],
        [getSize]
    );
}

export type DatabaseNotificationsWidgetTableRowItem = RowItem;
