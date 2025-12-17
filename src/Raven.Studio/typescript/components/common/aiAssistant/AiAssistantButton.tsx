import { ConditionalPopover } from "components/common/ConditionalPopover";
import "./AiAssistantButton.scss";
import { Icon } from "components/common/Icon";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import AiAssistantDisabledMessage from "components/common/aiAssistant/AiAssistantDisabledMessage";

interface AiAssistantButtonProps {
    handleClick: () => void;
    right?: string;
    bottom?: string;
}

export default function AiAssistantButton({ handleClick, right = "14px", bottom = "14px" }: AiAssistantButtonProps) {
    const aiAssistantSettings = useAppSelector(aiAssistantSelectors.settings);

    return (
        <ConditionalPopover
            conditions={{
                isActive: aiAssistantSettings.isDisabled,
                message: <AiAssistantDisabledMessage />,
            }}
            className="position-absolute"
            style={{
                right: right,
                bottom: bottom,
                zIndex: 5,
            }}
        >
            <Button
                variant="outline-secondary"
                className="rounded-pill ai-assistant-button"
                onClick={handleClick}
                disabled={aiAssistantSettings.isDisabled}
            >
                <Icon icon="ai-assistant" />
                AI Assistant
            </Button>
        </ConditionalPopover>
    );
}
