import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Switch } from "components/common/Checkbox";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import AiAssistantDisabledInSettingsMessage from "components/common/aiAssistant/AiAssistantDisabledInSettingsMessage";
import "./ChatbotHeader.scss";
import useConfirm from "components/common/ConfirmDialog";
import Badge from "react-bootstrap/Badge";

export default function ChatbotHeader() {
    const dispatch = useAppDispatch();
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);

    return (
        <div className="chatbot-header panel-bg-2 border-bottom border-color-light p-2 hstack justify-content-between align-items-center">
            <h4 className="m-0">
                <HeaderTitle />
            </h4>
            <div className="hstack gap-2">
                {chatbotTab === "aiAssistant" && <AskAiActions />}
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => dispatch(chatbotActions.isPinnedToggled())}
                    title={isPinned ? "Unpin window" : "Pin window"}
                >
                    <Icon icon={isPinned ? "pinned" : "pin"} margin="m-0" />
                </Button>
            </div>
        </div>
    );
}

function AskAiActions() {
    const dispatch = useAppDispatch();
    const isAlwaysAllowEndpointCalls = useAppSelector(chatbotSelectors.isAlwaysAllowEndpointCalls);
    const isChatbotDataSubmissionEnabled = useAppSelector(chatbotSelectors.isDataSubmissionEnabled);
    const aiAssistantSettings = useAppSelector(aiAssistantSelectors.settings);
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);

    const confirm = useConfirm();

    const isNewConversationDisabled = messagesCount === 0;

    const resetConversation = async () => {
        const isConfirmed = await confirm({
            title: "Start new conversation",
            message: "This will clear the current conversation.",
            confirmText: "Start new",
        });

        if (!isConfirmed) {
            return;
        }

        dispatch(chatbotActions.abortChat());
        dispatch(chatbotActions.messagesSet([]));
        dispatch(chatbotActions.conversationIdSet(null));
        dispatch(chatbotActions.attachedContextUnrelatedRemoved());
    };

    return (
        <>
            <ConditionalPopover
                conditions={{
                    isActive: isNewConversationDisabled,
                    message: "Chat is empty",
                }}
            >
                <Button
                    variant="link"
                    size="sm"
                    className="text-reset"
                    onClick={resetConversation}
                    disabled={isNewConversationDisabled}
                >
                    <Icon icon="plus" margin="m-0" title="Start new conversation" />
                </Button>
            </ConditionalPopover>
            <Dropdown>
                <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="link" className="text-emphasis">
                    <Icon icon="settings" margin="m-0" title="Chat settings" />
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
                    <ConditionalPopover
                        conditions={[
                            {
                                isActive: aiAssistantSettings.isDataSubmissionDisabled,
                                message: <AiAssistantDisabledInSettingsMessage />,
                            },
                        ]}
                    >
                        <Switch
                            color="primary"
                            selected={isChatbotDataSubmissionEnabled}
                            toggleSelection={() =>
                                dispatch(chatbotActions.isDataSubmissionEnabledSet(!isChatbotDataSubmissionEnabled))
                            }
                            disabled={aiAssistantSettings.isDataSubmissionDisabled}
                        >
                            Allow data submission
                        </Switch>
                    </ConditionalPopover>
                    <Button
                        variant="secondary"
                        onClick={() => dispatch(chatbotActions.exportConversation())}
                        className="d-block ps-0 rounded-1 w-100 text-center mt-2"
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
        dispatch(chatbotActions.chatbotResourcesTabSet("helpAndResources"));
    };

    if (chatbotTab === "aiAssistant") {
        return (
            <div className="d-flex align-items-center gap-1">
                <Icon icon="ask-ai" className="ai-gradient" margin="m-0" />
                AI Assistant
                <Badge
                    bg="faded-experimental"
                    className="font-monospace border border-experimental font-monospace font-size-10 px-1"
                    pill
                >
                    Experimental
                </Badge>
            </div>
        );
    }

    if (chatbotTab === "resources") {
        if (resourcesTab === "helpAndResources") {
            return (
                <div>
                    <Icon icon="resources" />
                    Resources
                </div>
            );
        }

        const resourcesTabTitles: Record<string, string> = {
            joinTheCommunity: "Join the Community",
            contactSupport: "Contact Support",
            submitFeedback: "Submit Feedback",
        };

        return (
            <div className="hstack gap-1">
                <Button variant="link" size="sm" className="text-reset p-0" onClick={handleResourcesGoBack}>
                    <Icon icon="arrow-thin-left" margin="m-0" />
                </Button>
                {resourcesTabTitles[resourcesTab] ?? resourcesTab}
            </div>
        );
    }

    return <div>{chatbotTab}</div>;
}
