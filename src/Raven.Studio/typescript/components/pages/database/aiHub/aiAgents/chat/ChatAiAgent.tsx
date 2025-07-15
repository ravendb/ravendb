import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEffect, useRef } from "react";
import AiAgentMessages from "../partials/AiAgentMessages";
import { Icon } from "components/common/Icon";
import ChatAiAgentInfoHub from "./ChatAiAgentInfoHub";
import { useForm, useWatch } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./utils/chatAiAgentValidation";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import { FormInput } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { AiAgentToolCall } from "../utils/aiAgentsTypes";
import SizeGetter from "components/common/SizeGetter";
import AceEditor from "components/common/ace/AceEditor";
import { Switch } from "components/common/Checkbox";
import Spinner from "react-bootstrap/Spinner";

interface QueryParams {
    agentId: string;
    conversationId: string;
}

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const messagesPanelRef = useRef<HTMLDivElement>(null);
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const runChatState = useAppSelector(chatAiAgentSelectors.runChatState);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);

    // Get data on load
    useEffect(() => {
        const getData = async () => {
            dispatch(chatAiAgentActions.conversationIdSet(queryParams?.conversationId));

            const config = await dispatch(
                chatAiAgentActions.getConfig({ databaseName, id: queryParams?.agentId })
            ).unwrap();

            setValue(
                "parameters",
                config.Parameters.map((x: string) => ({ name: x, value: "" }))
            );

            if (queryParams?.conversationId) {
                dispatch(chatAiAgentActions.getDocument({ databaseName, id: queryParams?.conversationId }));
            }
        };

        getData();

        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

    // Scroll to the bottom of the test panel when new messages are added
    useEffect(() => {
        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    const { control, handleSubmit, setValue } = useForm<ChatAiAgentFormData>({
        resolver: chatAiAgentYupResolver,
        defaultValues: {
            prompt: "",
            parameters: [],
        },
    });

    const formValues = useWatch({
        control,
    });

    const runChat = async (toolCallParameters?: AiAgentToolCall[]) => {
        await dispatch(
            chatAiAgentActions.runChat({
                databaseName,
                prompt: formValues.prompt,
                initialParameters: formValues.parameters,
                toolCallParameters,
            })
        ).unwrap();

        setValue("prompt", "");
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            runChat();
        });
    };

    if (!queryParams?.agentId) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
    }

    return (
        <div className="content-padding h-100 vstack">
            <div className="hstack justify-content-between align-items-start">
                <h2 className="text-truncate w-50 mb-3" title={config.data?.Name}>
                    <Icon icon="ai-agents" /> {config.data?.Name ?? "AI Agent"}{" "}
                </h2>
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack mb-2 justify-content-between">
                <a className="btn btn-secondary rounded-pill" href={appUrl.forAiAgents(databaseName)}>
                    <Icon icon="cancel" /> Cancel
                </a>
                <Switch
                    color="primary"
                    selected={isRawData}
                    toggleSelection={() => dispatch(chatAiAgentActions.isRawDataSet(!isRawData))}
                >
                    Raw data
                </Switch>
            </div>

            <div className="flex-grow-1">
                <SizeGetter
                    isHeighRequired
                    render={({ height }) => (
                        <form className="vstack overflow-auto" onSubmit={handleSubmit(handleSend)} style={{ height }}>
                            <div
                                ref={messagesPanelRef}
                                className="overflow-auto ps-2 flex-grow-1 position-relative"
                                style={{ height: height - promptHeightInPx }}
                            >
                                {messages.length === 0 && (
                                    <div className="p-5">
                                        <AiAgentParametersField
                                            control={control}
                                            name="parameters"
                                            value={formValues.parameters}
                                        />
                                    </div>
                                )}
                                {!isRawData && messages.length > 0 && (
                                    <AiAgentMessages
                                        messages={messages}
                                        toolQueries={config.data?.Queries}
                                        toolActions={config.data?.Actions}
                                        handleSaveParameters={(toolCallParameters) => runChat(toolCallParameters)}
                                    />
                                )}
                                {isRawData && document.data && (
                                    <AceEditor
                                        mode="json"
                                        value={JSON.stringify(document.data, null, 2)}
                                        height={`${height - promptHeightInPx}px`}
                                        readOnly
                                    />
                                )}
                                {isLoading && (
                                    <div className="position-absolute top-50 start-50 translate-middle">
                                        <Spinner animation="border" />
                                    </div>
                                )}
                            </div>
                            <div className="mt-3 px-2">
                                <div className="position-relative">
                                    <FormInput
                                        type="textarea"
                                        as="textarea"
                                        control={control}
                                        name="prompt"
                                        placeholder="Message an agent"
                                        rows={3}
                                        className="rounded-2"
                                        style={{ resize: "none" }}
                                        onKeyDown={(e) => {
                                            if (e.key === "Enter" && !e.shiftKey) {
                                                e.preventDefault();
                                                handleSubmit(handleSend)();
                                            }
                                        }}
                                        disabled={isLoading}
                                    />
                                    {formValues.prompt && (
                                        <ButtonWithSpinner
                                            type="submit"
                                            variant="secondary"
                                            icon="arrow-up"
                                            isSpinning={runChatState === "loading"}
                                            disabled={isLoading}
                                            className="position-absolute rounded-pill"
                                            style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                                        />
                                    )}
                                </div>
                            </div>
                        </form>
                    )}
                />
            </div>
        </div>
    );
}

const promptHeightInPx = 150;
