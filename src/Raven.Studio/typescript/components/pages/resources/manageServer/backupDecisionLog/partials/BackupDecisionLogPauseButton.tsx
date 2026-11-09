import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useConfirm from "components/common/ConfirmDialog";
import { useServices } from "components/hooks/useServices";

interface BackupDecisionLogPauseButtonProps {
    nodeTag: string;
    isPaused: boolean;
}

export default function BackupDecisionLogPauseButton({ nodeTag, isPaused }: BackupDecisionLogPauseButtonProps) {
    const { manageServerService } = useServices();
    const confirm = useConfirm();

    const asyncPause = useAsyncCallback(async () => {
        const isConfirmed = await confirm({
            title: `Pause backup scheduler on node ${nodeTag}?`,
            message: "Scheduled backups won't start on this node until you resume it.",
            icon: "pause",
            confirmText: "Pause",
            actionColor: "warning",
        });

        if (isConfirmed) {
            await manageServerService.pauseBackupScheduler(nodeTag);
        }
    });

    const asyncResume = useAsyncCallback(() => manageServerService.resumeBackupScheduler(nodeTag));

    return isPaused ? (
        <ButtonWithSpinner
            variant="success"
            icon="play"
            isSpinning={asyncResume.loading}
            onClick={() => asyncResume.execute()}
        >
            Resume
        </ButtonWithSpinner>
    ) : (
        <ButtonWithSpinner
            variant="warning"
            icon="pause"
            isSpinning={asyncPause.loading}
            onClick={() => asyncPause.execute()}
        >
            Pause
        </ButtonWithSpinner>
    );
}
