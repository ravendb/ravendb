import moment from "moment";
import Button from "react-bootstrap/Button";
import alert from "common/notifications/models/alert";
import { Icon } from "components/common/Icon";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";

export default function NewVersionAvailableDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Server_NewVersionAvailable">>) {
    const { VersionInfo } = useKnockoutValue(notification.details);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsList>
                <NotificationDetailsListItem label="Build Type">{VersionInfo.BuildType}</NotificationDetailsListItem>
                <NotificationDetailsListItem label="Release date">
                    {moment(VersionInfo.PublishedAt).format("LLL")}
                </NotificationDetailsListItem>
            </NotificationDetailsList>
            <div className="hstack gap-2 justify-content-center">
                <Button href="https://ravendb.net/download" target="_blank" variant="primary" className="rounded-pill">
                    <Icon icon="export" />
                    Download
                </Button>
                <Button
                    href={`https://ravendb.net/whats-new?buildNumber=${VersionInfo.BuildNumber}`}
                    target="_blank"
                    variant="info"
                    className="rounded-pill"
                >
                    <Icon icon="info" />
                    What&apos;s New
                </Button>
            </div>
        </AlertDetailsModal>
    );
}
