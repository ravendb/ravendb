import { Icon } from "components/common/Icon";
import {
    chatbotActions,
    ChatbotAssistantMessage,
    ChatbotUserActionState,
    ChatbotUserMessage,
} from "../store/chatbotSlice";
import { LazyLoad } from "components/common/LazyLoad";
import moment from "moment";
import assertUnreachable from "components/utils/assertUnreachable";
import ReactMarkdown, { Components } from "react-markdown";
import Table from "react-bootstrap/Table";
import Code, { CodeLanguage, supportedCodeLanguages } from "components/common/Code";
import { isValidElement, useEffect, useMemo, useRef } from "react";
import remarkGfm from "remark-gfm";
import Button from "react-bootstrap/Button";
import RichAlert from "components/common/RichAlert";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import {
    AdditionalContextOption,
    ChatbotRelevantLink,
    RunChatbotAiAssistantResultDto,
} from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { Element } from "hast";
import { aiAssistantConstants } from "components/common/aiAssistant/aiAssistantConstants";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import useTypewriter from "components/hooks/useTypewriter";
import { DatabaseSwitcherOption } from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/databaseSwitcherTypes";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import NoDatabasePlaceholder from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/NoDatabasePlaceholder";
import { SelectOption } from "components/common/select/Select";
import DatabaseOptionItem from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseOptionItem";
import DatabaseSingleValue from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseSingleValue";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SubmitHandler, useForm } from "react-hook-form";
import { tryHandleSubmit } from "components/utils/common";
import { FormGroup, FormInput, FormLabel, FormSelect, FormSelectAutocomplete } from "components/common/Form";
import Form from "react-bootstrap/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ChatbotAskAiAttachedContext from "./askAi/ChatbotAskAiAttachedContext";
import Dropdown from "react-bootstrap/Dropdown";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { useAsyncCallback } from "react-async-hook";
import messagePublisher from "common/messagePublisher";
import Badge from "react-bootstrap/Badge";

export default function ChatbotMessages() {
    const messagesRef = useRef<HTMLDivElement>(null);

    const messageIds = useAppSelector(chatbotSelectors.messageIds);
    const oneBeforeLastMessageRole = useAppSelector(chatbotSelectors.oneBeforeLastMessageRole);

    // Scroll to the bottom when messages are updated
    useEffect(() => {
        const current = messagesRef.current;
        if (!current) {
            return;
        }

        let top = current.scrollHeight - current.clientHeight;

        if (oneBeforeLastMessageRole === "user") {
            top -= 50; // height to see last line of user message
        }

        current.scrollTo({ top, behavior: "smooth" });
    }, [messageIds.length]);

    return (
        <div ref={messagesRef} className="flex-grow-1 overflow-y-auto vstack gap-2 px-2">
            {messageIds.map((id) => (
                <AiAgentMessage key={id} id={id} />
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    id: string;
}

function AiAgentMessage({ id }: AiAgentMessageProps) {
    const message = useAppSelector((state) => chatbotSelectors.messageById(state, id));
    const role = message.role;

    switch (role) {
        case "user":
            return <UserMessage message={message} />;
        case "assistant":
            return <AgentMessage message={message} />;
        default:
            return assertUnreachable(role);
    }
}

interface UserMessageProps {
    message: ChatbotUserMessage;
}

function UserMessage({ message }: UserMessageProps) {
    return (
        <div className="hstack justify-content-end">
            <div
                className="text-emphasis bg-faded-primary p-2 border-radius-xs border border-primary"
                style={{ maxWidth: "75%" }}
            >
                <ChatbotAskAiAttachedContext attachedContexts={message.attachedContexts} isReadOnly className="mb-1" />
                <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </div>
            </div>
        </div>
    );
}

interface AgentMessageProps {
    message: ChatbotAssistantMessage;
}

function AgentMessage({ message }: AgentMessageProps) {
    const isLastMessage = useAppSelector((state) => chatbotSelectors.isLastMessage(state, message.id));

    return (
        <div style={{ minHeight: isLastMessage ? "-webkit-fill-available" : "unset" }}>
            <AgentMessageBody message={message} />
        </div>
    );
}

function AgentMessageBody({ message }: AgentMessageProps) {
    const dispatch = useAppDispatch();

    const contentTypewriter = useTypewriter({
        text: message.content,
    });

    if (message.state === "Loading") {
        return (
            <LazyLoad active>
                <div style={{ height: "100px", width: "100%" }} />
            </LazyLoad>
        );
    }

    if (message.state === "InvalidData") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidData}</RichAlert>;
    }

    if (message.state === "InvalidCredentials") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidCredentials}</RichAlert>;
    }

    if (message.state === "OutOfTokens") {
        return <RichAlert variant="danger">{aiAssistantConstants.outOfTokens}</RichAlert>;
    }

    if (message.state === "ConsentRequired") {
        return <AiAssistantConsentStatusChecker onConsentGiven={() => dispatch(chatbotActions.retryRunChat())} />;
    }

    if (message.state === "Error") {
        return (
            <RichAlert variant="danger">
                {message.errorMessage ?? "Failed to get response from AI Assistant."}{" "}
                <Button variant="link" className="px-0" onClick={() => dispatch(chatbotActions.retryRunChat())}>
                    Please try again
                </Button>
            </RichAlert>
        );
    }

    if (Object.keys(message.additionalContext).length > 0) {
        return (
            <AdditionalContext
                id={message.id}
                additionalContext={message.additionalContext}
                userActionState={message.userActionState}
            />
        );
    }

    if (Object.keys(message.endpoints).length > 0) {
        return <Endpoints id={message.id} endpoints={message.endpoints} userActionState={message.userActionState} />;
    }

    return (
        <div>
            {message.thinkingTimeInMs != null ? (
                <div className="text-muted">
                    Thought for {moment.duration(message.thinkingTimeInMs).asSeconds().toFixed(2)}s
                </div>
            ) : (
                <div className="text-muted">Thinking</div>
            )}
            <div className="mt-1">
                <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
                    {contentTypewriter}
                </ReactMarkdown>
            </div>
            <RelevantLinks links={message.relevantLinks} />
            <FollowUpQuestions questions={message.followUpQuestions} />
        </div>
    );
}

interface AdditionalContextProps {
    id: string;
    additionalContext: RunChatbotAiAssistantResultDto["AdditionalContext"];
    userActionState: ChatbotUserActionState;
}

function AdditionalContext({ id, additionalContext, userActionState }: AdditionalContextProps) {
    const dispatch = useAppDispatch();
    const allAdditionalContextOptions = Object.values(additionalContext).map((option) => option.Option);

    const isOption = (option: AdditionalContextOption) => allAdditionalContextOptions.includes(option);

    const getToolCallId = (option: AdditionalContextOption) => {
        return Object.keys(additionalContext).find((key) => additionalContext[key].Option === option);
    };

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    const databaseOptions: DatabaseSwitcherOption[] = useMemo(() => {
        const sortedByNameDatabases = allDatabases.sort((a, b) => a.name.localeCompare(b.name));
        const sortedByStatusDatabases = [
            ...sortedByNameDatabases.filter((item) => !item.isDisabled),
            ...sortedByNameDatabases.filter((item) => item.isDisabled),
        ];

        return sortedByStatusDatabases.map((db) => ({
            value: db.name,
            isSharded: db.isSharded,
            environment: db.environment,
            isDisabled: db.isDisabled,
        }));
    }, [allDatabases]);

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const { control, handleSubmit, formState } = useForm<ActionContextFormData>({
        defaultValues: {
            databaseName:
                isOption("DatabaseName") && activeDatabaseName
                    ? (databaseOptions?.find((x) => x.value === activeDatabaseName)?.value ?? null)
                    : null,
            collectionName: null,
            documentId: null,
            indexName: null,
        },
        resolver: yupResolver(actionContextSchema),
    });

    const handleSend: SubmitHandler<ActionContextFormData> = (data) => {
        return tryHandleSubmit(async () => {
            const actionResponses: Record<string, any> = {};

            if (isOption("DatabaseName")) {
                actionResponses[getToolCallId("DatabaseName")] = data.databaseName;
            }
            if (isOption("CollectionName")) {
                actionResponses[getToolCallId("CollectionName")] = data.collectionName;
            }
            if (isOption("DocumentId")) {
                actionResponses[getToolCallId("DocumentId")] = data.documentId;
            }
            if (isOption("IndexName")) {
                actionResponses[getToolCallId("IndexName")] = data.indexName;
            }

            dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "allowed" } }));
            dispatch(chatbotActions.runChat({ actionResponses }));
        });
    };

    const handleSkip = () => {
        const actionResponses: Record<string, any> = {};

        for (const option of allAdditionalContextOptions) {
            actionResponses[getToolCallId(option)] = "Skipped";
        }

        dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "skipped" } }));
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    return (
        <Form className="well border border-secondary rounded-2" onSubmit={handleSubmit(handleSend)}>
            <div className="fs-6 py-1 px-2 border-bottom border-secondary">
                <Icon icon="about" />
                Additional context
            </div>
            <div className="p-2">
                {isOption("DatabaseName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Database</FormLabel>
                            <FormSelect
                                control={control}
                                name="databaseName"
                                placeholder={<NoDatabasePlaceholder />}
                                options={databaseOptions}
                                components={{ Option: DatabaseOptionItem, SingleValue: DatabaseSingleValue }}
                                isRoundedPill
                            />
                        </FormGroup>
                    </div>
                )}
                {isOption("CollectionName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Collection</FormLabel>
                            <FormSelectAutocomplete
                                control={control}
                                name="collectionName"
                                options={collectionOptions}
                                isRoundedPill
                            />
                        </FormGroup>
                    </div>
                )}
                {isOption("DocumentId") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Document ID</FormLabel>
                            <FormInput type="text" control={control} name="documentId" className="rounded-pill" />
                        </FormGroup>
                    </div>
                )}
                {isOption("IndexName") && (
                    <div>
                        <FormGroup>
                            <FormLabel>Index name</FormLabel>
                            <FormInput type="text" control={control} name="indexName" className="rounded-pill" />
                        </FormGroup>
                    </div>
                )}
                <div className="hstack justify-content-end mt-2">
                    {userActionState === "waiting" && (
                        <div className="hstack gap-1">
                            <Button variant="link" className="text-emphasis" size="xs" onClick={handleSkip}>
                                Skip
                            </Button>
                            <ButtonWithSpinner
                                variant="primary"
                                type="submit"
                                isSpinning={formState.isSubmitting}
                                className="rounded-pill"
                                size="sm"
                            >
                                Send
                            </ButtonWithSpinner>
                        </div>
                    )}
                    {userActionState === "skipped" && (
                        <Badge bg="secondary" className="rounded-pill">
                            <Icon icon="skip" />
                            Skipped
                        </Badge>
                    )}
                    {userActionState === "allowed" && (
                        <Badge bg="success" className="rounded-pill">
                            <Icon icon="check" />
                            Success
                        </Badge>
                    )}
                </div>
            </div>
        </Form>
    );
}

const actionContextSchema = yup.object({
    databaseName: yup.string().nullable(),
    collectionName: yup.string().nullable(),
    documentId: yup.string().nullable(),
    indexName: yup.string().nullable(),
});

type ActionContextFormData = yup.InferType<typeof actionContextSchema>;

interface EndpointsProps {
    id: string;
    endpoints: RunChatbotAiAssistantResultDto["Endpoints"];
    userActionState: ChatbotUserActionState;
}

function Endpoints({ id, endpoints, userActionState }: EndpointsProps) {
    const dispatch = useAppDispatch();

    const deniedEndpoints = useAppSelector(chatbotSelectors.deniedEndpoints);
    const alwaysAllowedEndpoints = useAppSelector(chatbotSelectors.alwaysAllowedEndpoints);

    const endpointsArray = Object.values(endpoints).flatMap((x) => x);

    const hasOnlyDeniedEndpoints = endpointsArray.every((endpoint) => deniedEndpoints.includes(endpoint));
    const hasOnlyAllowedEndpoints = endpointsArray.every((endpoint) => alwaysAllowedEndpoints.includes(endpoint));

    const asyncHandleAllow = useAsyncCallback(
        async () => {
            const actionResponses: Record<string, any> = {};

            for (const toolId of Object.keys(endpoints)) {
                for (const endpoint of endpoints[toolId]) {
                    const response = await fetch(endpoint);
                    const data = await response.json();

                    actionResponses[toolId] = {
                        [endpoint]: data,
                    };

                    dispatch(
                        chatbotActions.attachedContextAdded({
                            id: `endpoint-${_.uniqueId()}`,
                            type: "Endpoints Responses",
                            label: endpoint,
                            value: JSON.stringify(data),
                            state: "included",
                        })
                    );
                }
            }

            dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "allowed" } }));
            dispatch(chatbotActions.runChat({ actionResponses }));
        },
        {
            onError: () => {
                messagePublisher.reportError("Failed to retrieve endpoints");
            },
        }
    );

    const handleAlwaysAllow = async () => {
        dispatch(chatbotActions.alwaysAllowedEndpointsAdded(endpointsArray));
        asyncHandleAllow.execute();
    };

    const handleSkip = () => {
        const actionResponses: Record<string, any> = {};

        for (const toolId of Object.keys(endpoints)) {
            for (const endpoint of endpoints[toolId]) {
                actionResponses[toolId] = {
                    [endpoint]: "Skipped",
                };
            }
        }

        dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "skipped" } }));
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    const handleDeny = () => {
        dispatch(chatbotActions.deniedEndpointsAdded(endpointsArray));
        handleSkip();
    };

    useEffect(() => {
        if (hasOnlyAllowedEndpoints) {
            asyncHandleAllow.execute();
        }
    }, []);

    useEffect(() => {
        if (hasOnlyDeniedEndpoints) {
            handleSkip();
        }
    }, []);

    return (
        <div className="well border border-secondary rounded-2">
            <div className="fs-6 py-1 px-2 border-bottom border-secondary">
                <Icon icon="endpoint" />
                Retrieve endpoints
            </div>
            <div className="p-2">
                <ul className="vstack gap-1 ps-3">
                    {endpointsArray.map((endpoint) => (
                        <li key={endpoint} className="text-break">
                            GET {endpoint}
                        </li>
                    ))}
                </ul>
                {userActionState === "waiting" ? (
                    <div className="hstack justify-content-between mt-2">
                        <Button
                            variant="secondary"
                            className="rounded-pill"
                            size="sm"
                            onClick={handleDeny}
                            disabled={asyncHandleAllow.loading}
                        >
                            Deny
                        </Button>
                        <div className="hstack gap-1">
                            <Button
                                variant="link"
                                className="text-emphasis"
                                size="xs"
                                onClick={handleSkip}
                                disabled={asyncHandleAllow.loading}
                            >
                                Skip
                            </Button>
                            <Dropdown className="button-dropdown-pill" as={ButtonGroup}>
                                <ButtonWithSpinner
                                    variant="primary"
                                    className="button-dropdown-btn"
                                    size="sm"
                                    onClick={asyncHandleAllow.execute}
                                    isSpinning={asyncHandleAllow.loading}
                                >
                                    Allow
                                </ButtonWithSpinner>
                                <Dropdown.Toggle
                                    variant="primary"
                                    className="dropdown-toggle button-dropdown-toggle"
                                    as={CustomDropdownToggle}
                                    size="sm"
                                    disabled={asyncHandleAllow.loading}
                                />
                                <Dropdown.Menu>
                                    <Dropdown.Item onClick={handleAlwaysAllow} className="fs-5">
                                        Always allow
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        </div>
                    </div>
                ) : (
                    <div className="hstack justify-content-end mt-2">
                        {userActionState === "skipped" && (
                            <Badge bg="secondary" className="rounded-pill">
                                <Icon icon="skip" />
                                Skipped
                            </Badge>
                        )}
                        {userActionState === "allowed" && (
                            <Badge bg="success" className="rounded-pill">
                                <Icon icon="check" />
                                Success
                            </Badge>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}

const markdownComponents: Components = {
    h1: ({ children }) => {
        return <h1 className="mt-2">{children}</h1>;
    },
    h2: ({ children }) => {
        return <h2 className="mt-2">{children}</h2>;
    },
    h3: ({ children }) => {
        return <h3 className="mt-2">{children}</h3>;
    },
    h4: ({ children }) => {
        return <h4 className="mt-2">{children}</h4>;
    },
    h5: ({ children }) => {
        return <h5 className="mt-2">{children}</h5>;
    },
    h6: ({ children }) => {
        return <h6 className="mt-2">{children}</h6>;
    },
    a: ({ children, href }) => (
        <a href={href} target="_blank">
            {children}
        </a>
    ),
    table: ({ children }) => (
        <Table bordered striped hover className="mb-2">
            {children}
        </Table>
    ),
    blockquote: ({ children }) => (
        <blockquote className="blockquote fs-4 p-2 panel-bg-2 rounded-2">{children}</blockquote>
    ),
    pre: ({ node, children }) => {
        const languageFromNode = getLanguageFromNode(node);
        const childrenData = getChildrenData(children);

        const code = childrenData.code ?? "";
        const language = getCodeLanguage(childrenData.language ?? languageFromNode);

        return <Code code={code} language={language} className="mb-2" />;
    },
};

function getLanguageFromNode(node: Element): string {
    const className = node.properties.className;

    if (typeof className === "string") {
        return getLanguageFromClassName(className);
    }

    const children = node.children[0] as Element;
    const childrenClassName = children.properties.className;

    if (typeof childrenClassName === "string") {
        return getLanguageFromClassName(childrenClassName);
    }

    if (Array.isArray(childrenClassName) && typeof childrenClassName[0] === "string") {
        return getLanguageFromClassName(childrenClassName[0]);
    }

    return null;
}

function getChildrenData(children: React.ReactNode): { language: string; code: string } {
    if (!isValidElement(children) || !children.props) {
        return { language: null, code: null };
    }

    const props = children.props as any;

    let language: string = null;
    if (typeof props.className === "string") {
        language = getLanguageFromClassName(props.className);
    }

    let code: string = null;
    if (typeof props.children === "string") {
        code = props.children;
    } else if (typeof children === "string") {
        code = children;
    }

    return { language, code };
}

function getLanguageFromClassName(className: string): string {
    if (typeof className !== "string") {
        return null;
    }

    return className.replace("language-", "");
}

function getCodeLanguage(language: string): CodeLanguage {
    if (supportedCodeLanguages.includes(language as CodeLanguage)) {
        return language as CodeLanguage;
    }

    switch (language) {
        case "js":
            return "javascript";
        case "cs":
        case "dotnet":
            return "csharp";
        default:
            return "plaintext";
    }
}

function RelevantLinks({ links }: { links: ChatbotRelevantLink[] }) {
    if (!links?.length) {
        return null;
    }

    return (
        <div className="hstack gap-1 flex-wrap mt-1">
            {links.filter(Boolean).map((link) => (
                <a
                    key={link.Url}
                    href={link.Url}
                    target="_blank"
                    className="btn btn-sm rounded-pill py-1 px-2 panel-bg-2 border border-secondary text-reset"
                >
                    <Icon icon="raven" size="sm" color="info" />
                    {link.Title}
                </a>
            ))}
        </div>
    );
}

function FollowUpQuestions({ questions }: { questions: string[] }) {
    const dispatch = useAppDispatch();

    if (!questions?.length) {
        return null;
    }

    return (
        <div className="mt-2">
            <span className="small-label">Follow up questions</span>
            <div className="vstack gap-1">
                {questions.filter(Boolean).map((question) => (
                    <div
                        key={question}
                        className="py-1 px-2 rounded-3 border border-primary cursor-pointer hover-filter"
                        onClick={() => dispatch(chatbotActions.runChat({ message: question }))}
                    >
                        {question}
                    </div>
                ))}
            </div>
        </div>
    );
}
