import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";

interface SummaryAlertsModalProps {
    databaseName: string;
    alerts: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];
    alertsCount: number;
    nodeTag?: string;
    onClose: () => void;
}

export function SummaryAlertsModal({ alerts, databaseName, nodeTag, alertsCount, onClose }: SummaryAlertsModalProps) {
    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="vstack">
                <div className="text-center">
                    <Icon icon="alerts" color="warning" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    Alerts <strong>({alertsCount.toLocaleString()})</strong>
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-2 pt-0">
                <div className="text-truncate">
                    <Icon icon="database" color="primary" />
                    <strong title={databaseName}>{databaseName}</strong>
                </div>
                {nodeTag && (
                    <div>
                        <Icon icon="node" color="node" />
                        <strong>{nodeTag}</strong>
                    </div>
                )}
                <div className="vstack gap-1 panel-bg-2 p-1 rounded-2 overflow-auto" style={{ maxHeight: "300px" }}>
                    {alerts.map((alert) => (
                        <div key={alert.Reason} className="d-flex align-items-center">
                            <Badge bg="warning" pill>
                                <Icon icon="alert" />
                                {alert.Count.toLocaleString()}
                            </Badge>
                            <span className="ms-1">{alert.PrettifiedReason ?? alert.Reason}</span>
                        </div>
                    ))}
                </div>
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

interface SummaryPerformanceHintsModalProps {
    databaseName: string;
    performanceHints: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];
    performanceHintsCount: number;
    nodeTag?: string;
    onClose: () => void;
}

export function SummaryPerformanceHintsModal({
    performanceHints,
    databaseName,
    nodeTag,
    performanceHintsCount,
    onClose,
}: SummaryPerformanceHintsModalProps) {
    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="vstack">
                <div className="text-center">
                    <Icon icon="performance" color="info" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    Performance hints <strong>({performanceHintsCount.toLocaleString()})</strong>
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-2 pt-0">
                <div className="text-truncate">
                    <Icon icon="database" color="primary" />
                    <strong title={databaseName}>{databaseName}</strong>
                </div>
                {nodeTag && (
                    <div>
                        <Icon icon="node" color="node" />
                        <strong>{nodeTag}</strong>
                    </div>
                )}
                <div className="vstack gap-1 panel-bg-2 p-1 rounded-2 overflow-auto" style={{ maxHeight: "300px" }}>
                    {performanceHints.map((performanceHint) => (
                        <div key={performanceHint.Reason} className="d-flex align-items-center">
                            <Badge bg="info" pill>
                                <Icon icon="performance" />
                                {performanceHint.Count.toLocaleString()}
                            </Badge>
                            <span className="ms-1">{performanceHint.PrettifiedReason ?? performanceHint.Reason}</span>
                        </div>
                    ))}
                </div>
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
