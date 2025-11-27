import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import AiAssistantUsagePercentageCircle from "components/common/aiAssistant/AiAssistantUsagePercentageCircle";

export default function ChatbotHeader() {
    const dispatch = useAppDispatch();
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);

    const resetConversation = () => {
        dispatch(chatbotActions.messagesSet([]));
        dispatch(chatbotActions.conversationIdSet(null));
        dispatch(
            chatbotActions.attachedContextTypesRemoved([
                "Current Document",
                "Current Index Definition",
                "Endpoints Response",
            ])
        );
    };

    return (
        <div className="chatbot-header panel-bg-2 border-bottom border-secondary p-2 hstack justify-content-between align-items-center">
            <h4 className="m-0">
                <HeaderTitle />
            </h4>
            <div className="hstack">
                {chatbotTab === "Ask AI" && (
                    <Button variant="link" size="sm" className="text-reset" onClick={resetConversation}>
                        <Icon icon="plus" margin="m-0" />
                    </Button>
                )}
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => dispatch(chatbotActions.isPinnedToggled())}
                    className={classNames({ "text-reset": !isPinned })}
                >
                    <Icon icon={isPinned ? "pinned" : "pin"} margin="m-0" />
                </Button>
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => dispatch(chatbotActions.isOpenToggled())}
                    className="text-reset"
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            </div>
        </div>
    );
}

function HeaderTitle() {
    const dispatch = useAppDispatch();

    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const resourcesTab = useAppSelector(chatbotSelectors.chatbotResourcesTab);

    const handleResourcesGoBack = () => {
        dispatch(chatbotActions.chatbotResourcesTabSet("Help and resources"));
    };

    if (chatbotTab === "Ask AI") {
        return (
            <div className="d-flex align-items-center gap-2">
                <div>
                    <Icon icon="ask-ai" className="ai-gradient" />
                    Ask AI
                </div>
                <AiAssistantUsagePercentageCircle />
            </div>
        );
    }

    if (chatbotTab === "Resources") {
        if (resourcesTab === "Help and resources") {
            return (
                <div>
                    <Icon icon="resources" />
                    Resources
                </div>
            );
        }

        return (
            <div>
                <Button variant="link" size="sm" className="text-reset p-0 pe-1" onClick={handleResourcesGoBack}>
                    <Icon icon="arrow-thin-left" margin="m-0" />
                </Button>
                {resourcesTab}
            </div>
        );
    }

    return <div>{chatbotTab}</div>;
}
