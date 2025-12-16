import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import IconName from "typings/server/icons";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { aiAssistantConstants } from "components/common/aiAssistant/aiAssistantConstants";

export default function ChatbotFooter() {
    const dispatch = useAppDispatch();

    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const isDataSubmissionDisabled = useAppSelector(aiAssistantSelectors.settings).isDataSubmissionDisabled;

    return (
        <div className="chatbot-footer panel-bg-2 border-top border-secondary p-2 hstack">
            <FooterItem
                icon="ask-ai"
                title="Ask AI"
                isActive={chatbotTab === "askAi"}
                handleClick={() => dispatch(chatbotActions.chatbotTabSet("askAi"))}
                disabledReason={isDataSubmissionDisabled ? aiAssistantConstants.disabledInSettings : undefined}
            />
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
    disabledReason?: string;
}

function FooterItem({ icon, title, isActive, handleClick, disabledReason }: FooterItemProps) {
    return (
        <ConditionalPopover
            conditions={{
                isActive: !!disabledReason,
                message: disabledReason,
            }}
        >
            <div
                className={classNames("rounded-2 px-3 py-1 vstack align-items-center justify-content-center", {
                    "panel-bg-3 border border-secondary": isActive,
                    "cursor-pointer": !isActive,
                })}
                onClick={handleClick}
            >
                <div>
                    <Icon icon={icon} margin="m-0" />
                </div>
                <div>{title}</div>
            </div>
        </ConditionalPopover>
    );
}
