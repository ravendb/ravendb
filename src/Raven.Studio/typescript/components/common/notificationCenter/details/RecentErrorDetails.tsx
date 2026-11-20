import Button from "react-bootstrap/Button";
import recentError from "common/notifications/models/recentError";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "./notificationDetailsProvider";
import { NotificationDetailsModal } from "./NotificationDetailsModal";
import { CloseButton, DetailsFooter, DismissButton } from "./NotificationDetailsFooters";

export default function RecentErrorDetails({ notification, actions }: NotificationDetailsProps<recentError>) {
    const messageHtml = useKnockoutValue(notification.longerMessage);
    const errorDetails = useKnockoutValue(notification.details);

    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            messageHtml={messageHtml}
            footer={
                <DetailsFooter
                    start={
                        <Button
                            href="https://issues.hibernatingrhinos.com/issues/RavenDB"
                            target="_blank"
                            variant="info"
                            className="rounded-pill"
                        >
                            <Icon icon="bug" />
                            Bug tracker
                        </Button>
                    }
                >
                    <CloseButton onClick={actions.close} />
                    {notification.canBeDismissed() && <DismissButton onClick={actions.dismiss} />}
                </DetailsFooter>
            }
        >
            {errorDetails && (
                <Code
                    code={errorDetails}
                    language="plaintext"
                    whiteSpace="pre-wrap"
                    className="notification-details-code"
                />
            )}
        </NotificationDetailsModal>
    );
}
