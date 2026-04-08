import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import { Icon } from "components/common/Icon";
import ChatAiAgentInfoHub from "./partials/ChatAiAgentInfoHub";
import { FormProvider } from "react-hook-form";
import SizeGetter from "components/common/SizeGetter";
import { Switch } from "components/common/Checkbox";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import ChatAiAgentFormBody from "./partials/ChatAiAgentFormBody";
import AiAgentParametersDropdown from "../partials/AiAgentParametersDropdown";
import useChatAiAgent, { ChatAiAgentQueryParams } from "./hooks/useChatAiAgent";
import genUtils from "common/generalUtils";
import Badge from "react-bootstrap/Badge";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";
import AiAgentLinkedConversationsDropdown from "../partials/AiAgentLinkedConversationsDropdown";

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<ChatAiAgentQueryParams>) {
    const { handleSend, reloadForm, handleNewChat, chatForm, asyncGetDefaultValues, handleSubmit, runChat } =
        useChatAiAgent(queryParams);

    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const isDocumentDeleted = useAppSelector(chatAiAgentSelectors.isDocumentDeleted);

    const title = config.data?.Name ?? "AI Agent";

    if (!queryParams?.agentId) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
    }

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error="Unable to load configuration" refresh={reloadForm} />;
    }

    return (
        <div className="h-100 vstack">
            <div className="hstack justify-content-between align-items-start px-3 pt-3">
                <div className="hstack gap-2 w-50 mb-3 align-items-center">
                    <h2 className="text-truncate m-0" title={title}>
                        <Icon icon="ai-agents" /> {title}
                    </h2>
                    {document.data?.TotalUsage && <TotalUsageBadge usage={document.data.TotalUsage} />}
                    {config.data?.Disabled && (
                        <Badge bg="warning" pill>
                            Disabled
                        </Badge>
                    )}
                </div>
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack mb-2 justify-content-between px-3">
                <div className="hstack gap-2 flex-wrap">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={handleNewChat}
                        title="Click to start a new chat with the LLM using this agent"
                    >
                        <Icon icon="plus" /> New chat
                    </Button>
                    {conversationId && !isDocumentDeleted && (
                        <a
                            href={appUrl.forEditDoc(conversationId, databaseName)}
                            className="btn btn-secondary rounded-pill"
                            target="_blank"
                        >
                            <Icon icon="document" /> Edit document
                        </a>
                    )}
                    {config.data && (
                        <a
                            href={appUrl.forEditAiAgent(databaseName, config.data.Identifier)}
                            className="btn btn-secondary rounded-pill"
                            target="_blank"
                        >
                            <Icon icon="settings" /> Edit configuration
                        </a>
                    )}
                    <a className="btn btn-secondary rounded-pill" href={appUrl.forAiAgents(databaseName)}>
                        <Icon icon="cancel" /> Cancel
                    </a>
                </div>
                <div className="hstack gap-2">
                    <Switch
                        color="primary"
                        selected={isRawData}
                        toggleSelection={() => dispatch(chatAiAgentActions.isRawDataSet(!isRawData))}
                        title="Toggle on to view the chat communication in raw data format"
                    >
                        Raw data
                    </Switch>
                    {document.data?.Parameters && <AiAgentParametersDropdown parameters={document.data.Parameters} />}
                    {document.data?.LinkedConversations && (
                        <AiAgentLinkedConversationsDropdown linkedConversations={document.data.LinkedConversations} />
                    )}
                </div>
            </div>

            <div
                className={classNames("flex-grow-1 hstack justify-content-center", { "pb-3": queryParams?.isHistory })}
            >
                <SizeGetter
                    isHeighRequired
                    render={({ height }) => (
                        <FormProvider {...chatForm}>
                            <form
                                className="vstack overflow-auto"
                                onSubmit={handleSubmit(handleSend)}
                                style={{ height }}
                            >
                                <ChatAiAgentFormBody
                                    height={height}
                                    handleSend={handleSend}
                                    runChat={runChat}
                                    isHistory={queryParams?.isHistory}
                                />
                            </form>
                        </FormProvider>
                    )}
                />
            </div>
        </div>
    );
}

function TotalUsageBadge({ usage }: { usage: Raven.Client.Documents.Operations.AI.AiUsage }) {
    return (
        <Badge bg="info" pill>
            <PopoverWithHoverWrapper
                placement="bottom"
                message={
                    <AiTokensUsagePopoverBody
                        prompt={usage.PromptTokens}
                        completion={usage.CompletionTokens}
                        cached={usage.CachedTokens}
                        reasoning={usage.ReasoningTokens}
                        total={usage.TotalTokens}
                    />
                }
            >
                <Icon icon="info" />
            </PopoverWithHoverWrapper>
            Tokens used: {genUtils.formatAiTokens(usage.TotalTokens)}
        </Badge>
    );
}
