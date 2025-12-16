import "./AiAssistantButton.scss";
import { Icon } from "components/common/Icon";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";

interface AiAssistantButtonProps {
    handleClick: () => void;
    right?: string;
    bottom?: string;
}

export default function AiAssistantButton({ handleClick, right = "14px", bottom = "14px" }: AiAssistantButtonProps) {
    const isAiAssistantDisabled = useAppSelector(aiAssistantSelectors.settings).isDisabled;

    if (isAiAssistantDisabled) {
        return null;
    }

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
            <Icon icon="ai-assistant" />
            AI Assistant
        </Button>
    );
}
