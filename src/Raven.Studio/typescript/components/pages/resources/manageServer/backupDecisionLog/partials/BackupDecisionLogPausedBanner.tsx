import moment from "moment";
import RichAlert from "components/common/RichAlert";

interface BackupDecisionLogPausedBannerProps {
    nodeTag: string;
    pausedAt: string | null;
}

export default function BackupDecisionLogPausedBanner({ nodeTag, pausedAt }: BackupDecisionLogPausedBannerProps) {
    const pausedSince = pausedAt ? ` since ${moment(pausedAt).format("HH:mm")}` : "";

    return (
        <RichAlert variant="warning" icon="pause" className="mb-4">
            {`Backup scheduler is paused on node ${nodeTag}${pausedSince}. Scheduled backups won't start on this node.`}
        </RichAlert>
    );
}
