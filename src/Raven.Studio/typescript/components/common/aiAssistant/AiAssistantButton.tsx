import "./AiAssistantButton.scss";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";

interface AiAssistantButtonProps {
    handleClick: () => void;
    right?: string;
    bottom?: string;
}

export default function AiAssistantButton({ handleClick, right = "14px", bottom = "14px" }: AiAssistantButtonProps) {
    return (
        <Button
            variant="outline-secondary"
            className="rounded-pill position-absolute ai-assistant-button"
            onClick={handleClick}
            style={{
                right: right,
                bottom: bottom,
                zIndex: 5,
            }}
        >
            <Icon icon="refine-ai" />
            AI Assistant
        </Button>
    );
}
