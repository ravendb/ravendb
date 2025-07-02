import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAppDispatch } from "components/store";
import { editAiAgentActions } from "./store/editAiAgentSlice";

export default function EditAiAgentFooter() {
    const dispatch = useAppDispatch();

    return (
        <div className="hstack justify-content-between">
            <Button variant="outline-secondary rounded-pill" title="Cancel configuration and return to the tasks list.">
                Cancel
            </Button>
            <div className="hstack gap-2">
                <Button
                    variant="info"
                    className="rounded-pill"
                    onClick={() => dispatch(editAiAgentActions.isTestOpenSet(true))}
                >
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
