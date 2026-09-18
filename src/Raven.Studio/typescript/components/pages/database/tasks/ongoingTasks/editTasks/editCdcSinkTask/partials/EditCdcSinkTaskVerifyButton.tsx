import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { RavenButtonVariants } from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { EditCdcSinkTaskVerification } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";

interface EditCdcSinkTaskVerifyButtonProps {
    asyncVerify: EditCdcSinkTaskVerification;
    isDisabled: boolean;
}

export default function EditCdcSinkTaskVerifyButton({ asyncVerify, isDisabled }: EditCdcSinkTaskVerifyButtonProps) {
    const buttonData = getStatusButtonData(asyncVerify);

    return (
        <PopoverWithHoverWrapper message="Runs the CDC flow once against the source database, reading one row from each configured table without saving anything.">
            <ButtonWithSpinner
                type="button"
                variant={buttonData.variant}
                className="rounded-pill"
                onClick={asyncVerify.execute}
                isSpinning={asyncVerify.loading}
                disabled={isDisabled}
                icon={buttonData.icon}
            >
                {buttonData.label}
            </ButtonWithSpinner>
        </PopoverWithHoverWrapper>
    );
}

function getStatusButtonData(asyncVerify: EditCdcSinkTaskVerification): {
    label: string;
    icon: IconName;
    variant: RavenButtonVariants;
} {
    const { loading, error, result } = asyncVerify;

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
