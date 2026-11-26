import alert from "common/notifications/models/alert";
import genUtils from "common/generalUtils";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";

type INotificationDetails = Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails;
type MessageDetails = Raven.Server.NotificationCenter.Notifications.Details.MessageDetails;
type ExceptionDetails = Raven.Server.NotificationCenter.Notifications.Details.ExceptionDetails;

export default function GenericAlertDetails({ notification, actions }: NotificationDetailsProps<alert>) {
    const details = useKnockoutValue(notification.details);
    const messageHtml = useKnockoutValue(notification.message);

    const isDetailsMessageShown = isMessageDetails(details) && !isSameText(toMessageHtml(details.Message), messageHtml);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            {isDetailsMessageShown && <div className="notification-details-text">{details.Message}</div>}
            {isExceptionDetails(details) && (
                <Code
                    code={details.Exception}
                    language="plaintext"
                    whiteSpace="pre-wrap"
                    className="notification-details-code"
                />
            )}
        </AlertDetailsModal>
    );
}

function isMessageDetails(details: INotificationDetails): details is MessageDetails {
    return details != null && "Message" in details;
}

function isExceptionDetails(details: INotificationDetails): details is ExceptionDetails {
    return details != null && "Exception" in details;
}

function toMessageHtml(text: string): string {
    return genUtils.nl2br(genUtils.escapeHtml(text ?? ""));
}

function isSameText(first: string, second: string): boolean {
    const normalize = (text: string) => (text ?? "").replace(/\s+/g, " ").trim();
    return normalize(first) === normalize(second);
}
