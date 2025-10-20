import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { useCallback, useMemo } from "react";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import SizeGetter from "components/common/SizeGetter";
import Badge from "react-bootstrap/Badge";
import databasesManager from "common/shell/databasesManager";
import notificationCenter from "common/notifications/notificationCenter";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";

type NotificationSummaryItem =
    Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem;

interface NotificationModalProps {
    databaseName: string;
    items: NotificationSummaryItem[];
    count: number;
    nodeTag?: string;
    onClose: () => void;
}

export function SummaryAlertsModal({ items, databaseName, nodeTag, count, onClose }: NotificationModalProps) {
    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header closeButton onCloseClick={onClose}>
                <h3>
                    <Icon icon="alerts" color="warning" />
                    Alerts
                </h3>
            </Modal.Header>
            <Modal.Body className="vstack gap-2 pt-0">
                <NotificationDetails type="alerts" databaseName={databaseName} count={count} node={nodeTag} />
                <SizeGetter
                    render={({ width }) => (
                        <NotificationTable
                            width={width}
                            notifications={items}
                            databaseName={databaseName}
                            onClose={onClose}
                        />
                    )}
                />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onClose} className="rounded-pill">
                    <Icon icon="close" />
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

export function SummaryPerformanceHintsModal({ databaseName, items, count, nodeTag, onClose }: NotificationModalProps) {
    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-info" size="lg">
            <Modal.Header closeButton onCloseClick={onClose}>
                <h3>
                    <Icon icon="performance" color="info" />
                    Performance hints
                </h3>
            </Modal.Header>
            <Modal.Body className="vstack gap-2 pt-0">
                <NotificationDetails type="performanceHints" databaseName={databaseName} count={count} node={nodeTag} />
                <SizeGetter
                    render={({ width }) => (
                        <NotificationTable
                            width={width}
                            notifications={items}
                            databaseName={databaseName}
                            onClose={onClose}
                        />
                    )}
                />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onClose} className="rounded-pill">
                    <Icon icon="close" />
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

interface NotificationDetailsProps {
    type: "alerts" | "performanceHints";
    databaseName: string;
    count: number;
    node?: string;
}

function NotificationDetails({ databaseName, count, type, node }: NotificationDetailsProps) {
    return (
        <div>
            <div className="hstack justify-content-between align-items-center">
                <div>Notification type</div>
                <NotificationTypeBadge type={type} />
            </div>
            <div>
                <hr className="m-0 mt-1" />
                <div className="hstack justify-content-between align-items-center mt-2">
                    <div>Total count</div>
                    <div>{count.toLocaleString()}</div>
                </div>
            </div>
            <div>
                <hr className="m-0 mt-1" />
                <div className="hstack justify-content-between align-items-center mt-2">
                    <div>Database name</div>
                    <div className="text-truncate" title={databaseName}>
                        {databaseName}
                    </div>
                </div>
            </div>
            {node && (
                <div>
                    <hr className="m-0 mt-1" />
                    <div className="hstack justify-content-between align-items-center mt-2">
                        <div>Node</div>
                        <div>
                            <Icon icon="node" color="node" />
                            Node {node}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

function NotificationTypeBadge({ type }: Pick<NotificationDetailsProps, "type">) {
    if (type === "alerts") {
        return (
            <Badge pill bg="warning">
                <Icon icon="alert" />
                Alerts
            </Badge>
        );
    }

    if (type === "performanceHints") {
        return (
            <Badge pill bg="info">
                <Icon icon="performance" />
                Performance hints
            </Badge>
        );
    }

    return null;
}

interface NotificationTableProps {
    width: number;
    notifications: NotificationSummaryItem[];
    databaseName: string;
    onClose: () => void;
}

function NotificationTable({ width, notifications, databaseName, onClose }: NotificationTableProps) {
    const columns = useColumns(width, databaseName, onClose);

    const table = useReactTable({
        data: notifications,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(notifications.length, 300);

    return (
        <div className="mt-2">
            <VirtualTable table={table} heightInPx={heightInPx} />
        </div>
    );
}

function useColumns(width: number, databaseName: string, onClose: () => void): ColumnDef<NotificationSummaryItem>[] {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(width);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const openNotificationCenterForDatabase = useCallback(() => {
        const db = databasesManager.default.getDatabaseByName(databaseName);
        if (!db) {
            throw new Error("Cannot open notification center for database: " + databaseName);
        }

        databasesManager.default.activate(db, { waitForNotificationCenterWebSocket: true });
        notificationCenter.instance.showNotifications.toggle();
        onClose();
    }, [databaseName, onClose]);

    return useMemo(
        () => [
            {
                header: "Reason",
                accessorFn: (x) => x.PrettifiedReason ?? x.Reason,
                cell: ({ getValue }) => {
                    const value = getValue<string>();

                    return (
                        <CellWithCopy value={value}>
                            <Button onClick={openNotificationCenterForDatabase} variant="link">
                                {value}
                            </Button>
                        </CellWithCopy>
                    );
                },
                size: getSize(50),
            },
            {
                header: "Count",
                accessorKey: "Count",
                cell: CellValueWrapper,
                size: getSize(50),
            },
        ],
        [getSize]
    );
}
