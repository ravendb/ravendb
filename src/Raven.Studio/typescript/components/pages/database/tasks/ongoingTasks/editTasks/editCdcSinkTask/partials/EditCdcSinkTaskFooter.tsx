import { Icon } from "components/common/Icon";
import EditCdcSinkTaskVerificationAlert from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerificationAlert";
import EditCdcSinkTaskVerifyButton from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerifyButton";
import Button from "react-bootstrap/Button";
import { EditCdcSinkTaskVerification } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";

interface EditCdcSinkTaskFooterProps {
    asyncVerify: EditCdcSinkTaskVerification;
    isDirty: boolean;
    isDisabled: boolean;
    onCancel: () => void;
}

export default function EditCdcSinkTaskFooter({
    asyncVerify,
    isDirty,
    isDisabled,
    onCancel,
}: EditCdcSinkTaskFooterProps) {
    return (
        <>
            <EditCdcSinkTaskVerificationAlert result={asyncVerify.result} className="px-3 pb-2" />
            <div className="hstack justify-content-between gap-2 py-2 px-3 border-top border-secondary">
                <Button variant="outline-secondary" className="rounded-pill" onClick={onCancel}>
                    Cancel
                </Button>
                <div className="hstack gap-2">
                    <EditCdcSinkTaskVerifyButton asyncVerify={asyncVerify} isDisabled={isDisabled} />
                    <Button
                        type="submit"
                        variant="primary"
                        className="rounded-pill"
                        disabled={!isDirty || isDisabled || asyncVerify.loading}
                    >
                        <Icon icon="save" />
                        Save task configuration
                    </Button>
                </div>
            </div>
        </>
    );
}
