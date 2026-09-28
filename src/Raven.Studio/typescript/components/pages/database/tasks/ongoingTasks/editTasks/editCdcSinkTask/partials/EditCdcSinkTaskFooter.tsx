import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import EditCdcSinkTaskVerificationAlert from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerificationAlert";
import EditCdcSinkTaskVerifyButton from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerifyButton";
import Button from "react-bootstrap/Button";
import {
    EditCdcSinkTaskVerification,
    useEditCdcSinkTaskWatchedVerification,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";

interface EditCdcSinkTaskFooterProps {
    asyncVerify: EditCdcSinkTaskVerification;
    isDirty: boolean;
    isSubmitting: boolean;
    isDisabled: boolean;
    onCancel: () => void;
}

export default function EditCdcSinkTaskFooter({
    asyncVerify,
    isDirty,
    isSubmitting,
    isDisabled,
    onCancel,
}: EditCdcSinkTaskFooterProps) {
    const watchedVerification = useEditCdcSinkTaskWatchedVerification(asyncVerify);

    return (
        <>
            <EditCdcSinkTaskVerificationAlert result={watchedVerification.result} className="px-3 pb-2" />
            <div className="hstack justify-content-between gap-2 py-2 px-3 border-top border-secondary">
                <Button variant="outline-secondary" className="rounded-pill" onClick={onCancel} disabled={isSubmitting}>
                    Cancel
                </Button>
                <div className="hstack gap-2">
                    <EditCdcSinkTaskVerifyButton
                        verification={watchedVerification}
                        onVerify={asyncVerify.execute}
                        isDisabled={isDisabled || isSubmitting}
                    />
                    <ButtonWithSpinner
                        type="submit"
                        variant="primary"
                        className="rounded-pill"
                        disabled={!isDirty || isDisabled || asyncVerify.loading}
                        isSpinning={isSubmitting}
                        icon="save"
                    >
                        Save task configuration
                    </ButtonWithSpinner>
                </div>
            </div>
        </>
    );
}
