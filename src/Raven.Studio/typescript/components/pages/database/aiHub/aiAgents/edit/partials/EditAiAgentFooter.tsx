import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAppDispatch } from "components/store";
import { editAiAgentActions } from "../store/editAiAgentSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export default function EditAiAgentFooter() {
    const dispatch = useAppDispatch();

    const { control, setValue, formState, trigger } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const { forCurrentDatabase } = useAppUrls();

    const handleOpenTest = async () => {
        const isValid = await trigger();
        if (!isValid) {
            return;
        }

        setValue(
            "test.parameters",
            formValues.parameters.map((x) => ({ name: x.name, value: "" }))
        );
        dispatch(editAiAgentActions.isTestOpenSet(true));
    };

    return (
        <div className="hstack justify-content-between">
            <a
                href={forCurrentDatabase.aiAgents()}
                className="btn btn-outline-secondary rounded-pill"
                title="Cancel configuration and return to the ai agents list."
            >
                Cancel
            </a>
            <div className="hstack gap-2">
                <PopoverWithHoverWrapper message="Click to test the agent in the test area.">
                    <Button variant="info" className="rounded-pill" onClick={handleOpenTest}>
                        <Icon icon="test" />
                        Test
                    </Button>
                </PopoverWithHoverWrapper>

                <ButtonWithSpinner
                    type="submit"
                    variant="primary"
                    className="rounded-pill"
                    icon="save"
                    isSpinning={formState.isSubmitting}
                >
                    Save
                </ButtonWithSpinner>
            </div>
        </div>
    );
}
