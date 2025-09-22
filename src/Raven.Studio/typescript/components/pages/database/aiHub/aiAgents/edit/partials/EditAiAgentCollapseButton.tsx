import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";

interface EditAiAgentCollapseButtonProps {
    isPanelOpen: boolean;
    toggleIsPanelOpen: () => void;
}

export default function EditAiAgentCollapseButton({ isPanelOpen, toggleIsPanelOpen }: EditAiAgentCollapseButtonProps) {
    return (
        <Button variant="link" size="xs" onClick={toggleIsPanelOpen} className="text-reset">
            {isPanelOpen ? (
                <Icon icon="collapse-vertical" margin="m-0" />
            ) : (
                <Icon icon="expand-vertical" margin="m-0" />
            )}
        </Button>
    );
}
