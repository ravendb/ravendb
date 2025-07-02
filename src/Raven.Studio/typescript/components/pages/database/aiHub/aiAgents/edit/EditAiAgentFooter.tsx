import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAppDispatch } from "components/store";
import { editAiAgentActions } from "./store/editAiAgentSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";

export default function EditAiAgentFooter() {
    const dispatch = useAppDispatch();

    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const { forCurrentDatabase } = useAppUrls();

    const handleOpenTest = () => {
        setValue(
            "testParameters",
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
                <Button variant="info" className="rounded-pill" onClick={handleOpenTest}>
                    <Icon icon="test" />
                    Test
                </Button>

                <Button type="submit" variant="primary" className="rounded-pill">
                    <Icon icon="save" />
                    Save
                </Button>
            </div>
        </div>
    );
}
