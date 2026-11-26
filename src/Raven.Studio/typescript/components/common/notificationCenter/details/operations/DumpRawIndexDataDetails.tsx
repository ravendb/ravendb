import { useEffect } from "react";
import operation from "common/notifications/models/operation";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { NotificationDetailsModal } from "../NotificationDetailsModal";
import { CloseDetailsFooter } from "../NotificationDetailsFooters";
import { OperationFailure, OperationTimeElapsed } from "./OperationDetailsParts";

export default function DumpRawIndexDataDetails({ notification, actions }: NotificationDetailsProps<operation>) {
    const status = useKnockoutValue(notification.status);

    useEffect(() => {
        const subscription = notification.status.subscribe((newStatus) => {
            if (newStatus === "Canceled") {
                actions.close();
            }
        });
        return () => subscription.dispose();
    }, [notification, actions]);

    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            titleAddon={<OperationTimeElapsed operation={notification} />}
            footer={<CloseDetailsFooter actions={actions} />}
        >
            {status === "Faulted" && <OperationFailure operation={notification} />}
        </NotificationDetailsModal>
    );
}
