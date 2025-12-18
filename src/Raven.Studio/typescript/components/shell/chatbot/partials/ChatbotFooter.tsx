import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import IconName from "typings/server/icons";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import AiAssistantDisabledInSettingsMessage from "components/common/aiAssistant/AiAssistantDisabledInSettingsMessage";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";

export default function ChatbotFooter() {
    const dispatch = useAppDispatch();

    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const isAiAssistantSettingsDisabled = useAppSelector(aiAssistantSelectors.settings).isDisabled;
    const hasAiAssistant = useAppSelector(licenseSelectors.statusValue("HasAiAssistant"));

    return (
        <div className="chatbot-footer panel-bg-2 border-top border-secondary p-2 hstack">
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
                className="flex-grow-1"
            >
                <FooterItem
                    icon="ask-ai"
                    title="Ask AI"
                    isActive={chatbotTab === "askAi"}
                    handleClick={() => dispatch(chatbotActions.chatbotTabSet("askAi"))}
                    isDisabled={isAiAssistantSettingsDisabled || !hasAiAssistant}
                />
            </ConditionalPopover>
            <FooterItem
                icon="resources"
                title="Resources"
                isActive={chatbotTab === "resources"}
                handleClick={() => dispatch(chatbotActions.chatbotTabSet("resources"))}
            />
        </div>
    );
}

interface FooterItemProps {
    icon: IconName;
    title: string;
    isActive: boolean;
    handleClick: () => void;
    isDisabled?: boolean;
}

function FooterItem({ icon, title, isActive, handleClick, isDisabled }: FooterItemProps) {
    return (
        <div
            className={classNames("rounded-2 px-3 py-1 vstack align-items-center justify-content-center", {
                "panel-bg-3 border border-secondary": isActive,
                "cursor-pointer": !isActive && !isDisabled,
                "cursor-not-allowed opacity-50": isDisabled,
            })}
            onClick={isDisabled ? undefined : handleClick}
        >
            <div>
                <Icon icon={icon} margin="m-0" />
            </div>
            <div>{title}</div>
        </div>
    );
}
