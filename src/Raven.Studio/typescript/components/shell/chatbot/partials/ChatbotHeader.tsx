import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import AiAssistantUsagePercentageCircle from "components/common/aiAssistant/AiAssistantUsagePercentageCircle";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Switch } from "components/common/Checkbox";

export default function ChatbotHeader() {
    const dispatch = useAppDispatch();
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);

    return (
        <div className="chatbot-header panel-bg-2 border-bottom border-secondary p-2 hstack justify-content-between align-items-center">
            <h4 className="m-0">
                <HeaderTitle />
            </h4>
            <div className="hstack">
                {chatbotTab === "Ask AI" && <AskAiActions />}
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

function AskAiActions() {
    const dispatch = useAppDispatch();
    const isAlwaysAllowEndpointCalls = useAppSelector(chatbotSelectors.isAlwaysAllowEndpointCalls);

    const resetConversation = () => {
        dispatch(chatbotActions.messagesSet([]));
        dispatch(chatbotActions.conversationIdSet(null));
        dispatch(chatbotActions.attachedContextUnrelatedRemoved());
    };

    return (
        <>
            <Button variant="link" size="sm" className="text-reset" onClick={resetConversation}>
                <Icon icon="plus" margin="m-0" />
            </Button>
            <Dropdown>
                <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="link" className="text-emphasis">
                    <Icon icon="settings" margin="m-0" />
                </Dropdown.Toggle>
                <Dropdown.Menu className="p-3" style={{ minWidth: "max-content" }}>
                    <Switch
                        color="primary"
                        selected={isAlwaysAllowEndpointCalls}
                        toggleSelection={() =>
                            dispatch(chatbotActions.isAlwaysAllowEndpointCallsSet(!isAlwaysAllowEndpointCalls))
                        }
                    >
                        Always allow endpoints calls
                    </Switch>
                    <Button
                        variant="outline-secondary"
                        onClick={() => dispatch(chatbotActions.exportConversation())}
                        className="d-block ps-0 rounded-2 w-100 text-center mt-2"
                    >
                        <Icon icon="export" />
                        Export conversation
                    </Button>
                </Dropdown.Menu>
            </Dropdown>
        </>
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
