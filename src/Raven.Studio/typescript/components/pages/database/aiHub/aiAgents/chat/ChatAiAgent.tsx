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

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<ChatAiAgentQueryParams>) {
    const { handleSend, reloadForm, handleNewChat, chatForm, asyncGetDefaultValues, handleSubmit, runChat } =
        useChatAiAgent(queryParams);

    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);

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
                <h2 className="text-truncate w-50 mb-3" title={config.data?.Name}>
                    <Icon icon="ai-agents" /> {config.data?.Name ?? "AI Agent"}{" "}
                </h2>
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack mb-2 justify-content-between px-3">
                <div className="hstack gap-2">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={handleNewChat}
                        title="Click to start a new chat with the LLM using this agent"
                    >
                        <Icon icon="plus" /> New chat
                    </Button>
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
