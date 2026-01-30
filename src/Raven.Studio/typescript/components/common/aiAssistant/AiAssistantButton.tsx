import { ConditionalPopover } from "components/common/ConditionalPopover";
import "./AiAssistantButton.scss";
import { Icon } from "components/common/Icon";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import AiAssistantDisabledInSettingsMessage from "components/common/aiAssistant/AiAssistantDisabledInSettingsMessage";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { licenseSelectors } from "components/common/shell/licenseSlice";

interface AiAssistantButtonProps {
    handleClick: () => void;
    right?: string;
    bottom?: string;
}

export default function AiAssistantButton({ handleClick, right = "14px", bottom = "14px" }: AiAssistantButtonProps) {
    const isAiAssistantSettingsDisabled = useAppSelector(aiAssistantSelectors.settings).isDisabled;
    const hasAiAssistant = useAppSelector(licenseSelectors.statusValue("HasAiAssistant"));

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: isAiAssistantSettingsDisabled,
                    message: <AiAssistantDisabledInSettingsMessage />,
                },
                {
                    isActive: !hasAiAssistant,
                    message: <FeatureNotAvailableInYourLicensePopoverBody />,
                },
            ]}
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
                disabled={isAiAssistantSettingsDisabled || !hasAiAssistant}
            >
                <Icon icon="ai-assistant" />
                AI Assistant
            </Button>
        </ConditionalPopover>
    );
}
