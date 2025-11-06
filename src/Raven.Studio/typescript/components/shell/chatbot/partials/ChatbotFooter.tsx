import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import IconName from "typings/server/icons";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";

export default function ChatbotFooter() {
    const dispatch = useAppDispatch();

    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);

    return (
        <div className="chatbot-footer panel-bg-2 border-top border-secondary p-2 hstack">
            <FooterItem
                icon="chatbot"
                title="Ask AI"
                isActive={chatbotTab === "Ask AI"}
                handleClick={() => dispatch(chatbotActions.chatbotTabSet("Ask AI"))}
            />
            <FooterItem
                icon="resources"
                title="Resources"
                isActive={chatbotTab === "Resources"}
                handleClick={() => dispatch(chatbotActions.chatbotTabSet("Resources"))}
            />
        </div>
    );
}

interface FooterItemProps {
    icon: IconName;
    title: string;
    isActive: boolean;
    handleClick: () => void;
}

function FooterItem({ icon, title, isActive, handleClick }: FooterItemProps) {
    return (
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
    );
}
