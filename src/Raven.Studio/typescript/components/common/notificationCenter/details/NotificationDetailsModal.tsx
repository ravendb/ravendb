import { ReactNode } from "react";
import classNames from "classnames";
import Modal from "components/common/Modal";
import abstractNotification from "common/notifications/models/abstractNotification";
import alert from "common/notifications/models/alert";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "./notificationDetailsProvider";
import { AlertDetailsFooter, PerformanceHintDetailsFooter } from "./NotificationDetailsFooters";
import "./NotificationDetails.scss";

type DetailsModalProps<T extends abstractNotification> = NotificationDetailsProps<T> &
    Omit<NotificationDetailsModalProps, "notification" | "onClose" | "footer">;

export function AlertDetailsModal({ notification, actions, ...props }: DetailsModalProps<alert>) {
    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            footer={<AlertDetailsFooter alert={notification} actions={actions} />}
            {...props}
        />
    );
}

export function PerformanceHintDetailsModal({ notification, actions, ...props }: DetailsModalProps<performanceHint>) {
    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            footer={<PerformanceHintDetailsFooter hint={notification} actions={actions} />}
            {...props}
        />
    );
}

interface NotificationDetailsModalProps {
    notification: abstractNotification;
    onClose: () => void;
    footer: ReactNode;
    children?: ReactNode;
    title?: string;
    titleAddon?: ReactNode;
    messageHtml?: string;
    isMessageHidden?: boolean;
    size?: "xl" | "xxl";
}

export function NotificationDetailsModal({
    notification,
    onClose,
    footer,
    children,
    title,
    titleAddon,
    messageHtml,
    isMessageHidden,
    size = "xl",
}: NotificationDetailsModalProps) {
    const notificationTitle = useKnockoutValue(notification.title);
    const notificationMessage = useKnockoutValue(notification.message);
    const severityColor = useKnockoutValue(notification.severityColor);
    const displayDate = useKnockoutValue(notification.displayDate);

    const shownMessageHtml = isMessageHidden ? null : (messageHtml ?? notificationMessage);

    return (
        <Modal
            size={size}
            show
            onHide={onClose}
            className={classNames("notification-details modal-border", `bulge-${severityColor ?? "primary"}`)}
        >
            <Modal.Header closeButton onCloseClick={onClose}>
                <div className="hstack align-items-start gap-3 w-100 pe-4">
                    <div className="flex-grow-1">
                        <h3 className={classNames("mb-1", severityColor && `text-${severityColor}`)}>
                            {title ?? notificationTitle}
                        </h3>
                        <div className="text-muted small">{displayDate.format("LLL")}</div>
                    </div>
                    {titleAddon}
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-3 pt-0">
                {shownMessageHtml && (
                    <div
                        className="notification-details-message"
                        dangerouslySetInnerHTML={{ __html: shownMessageHtml }}
                    />
                )}
                {children}
            </Modal.Body>
            {footer}
        </Modal>
    );
}
