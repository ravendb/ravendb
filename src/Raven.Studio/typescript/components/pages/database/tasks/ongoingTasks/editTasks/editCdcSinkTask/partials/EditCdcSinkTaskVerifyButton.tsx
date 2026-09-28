import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { RavenButtonVariants } from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { EditCdcSinkTaskWatchedVerification } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";

interface EditCdcSinkTaskVerifyButtonProps {
    verification: EditCdcSinkTaskWatchedVerification;
    onVerify: () => void;
    isDisabled: boolean;
}

export default function EditCdcSinkTaskVerifyButton({
    verification,
    onVerify,
    isDisabled,
}: EditCdcSinkTaskVerifyButtonProps) {
    const buttonData = getStatusButtonData(verification);

    return (
        <PopoverWithHoverWrapper message="Runs the CDC flow once against the source database, reading one row from each configured table. Nothing is saved in RavenDB. CDC objects the run needs on the source database are created temporarily and removed afterwards.">
            <ButtonWithSpinner
                type="button"
                variant={buttonData.variant}
                className="rounded-pill"
                onClick={onVerify}
                isSpinning={verification.loading}
                disabled={isDisabled}
                icon={buttonData.icon}
            >
                {buttonData.label}
            </ButtonWithSpinner>
        </PopoverWithHoverWrapper>
    );
}

function getStatusButtonData({ loading, error, result }: EditCdcSinkTaskWatchedVerification): {
    label: string;
    icon: IconName;
    variant: RavenButtonVariants;
} {
    if (loading) {
        return { label: "Verifying tables...", icon: "shield", variant: "outline-info" };
    }
    if (error || (result && !result.Success)) {
        return { label: "Verification failed", icon: "danger", variant: "outline-danger" };
    }
    if (result?.Warnings.length) {
        return { label: "Tables verified with warnings", icon: "warning", variant: "outline-warning" };
    }
    if (result) {
        return { label: "Tables verified", icon: "check", variant: "outline-success" };
    }
    return { label: "Verify tables", icon: "shield", variant: "outline-info" };
}
