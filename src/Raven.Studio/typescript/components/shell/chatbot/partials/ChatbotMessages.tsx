import { Icon } from "components/common/Icon";
import { chatbotActions, ChatbotAssistantMessage, ChatbotUserMessage } from "../store/chatbotSlice";
import { LazyLoad } from "components/common/LazyLoad";
import moment from "moment";
import assertUnreachable from "components/utils/assertUnreachable";
import ReactMarkdown, { Components } from "react-markdown";
import Table from "react-bootstrap/Table";
import Code, { CodeLanguage, supportedCodeLanguages } from "components/common/Code";
import { isValidElement, useEffect, useRef } from "react";
import remarkGfm from "remark-gfm";
import Button from "react-bootstrap/Button";
import RichAlert from "components/common/RichAlert";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import { ChatbotRelevantLink } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { Element } from "hast";
import { aiAssistantConstants } from "components/common/aiAssistant/aiAssistantConstants";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import useTypewriter from "components/hooks/useTypewriter";

export default function ChatbotMessages() {
    const messagesRef = useRef<HTMLDivElement>(null);

    const messageIds = useAppSelector(chatbotSelectors.messageIds);

    // Scroll to the bottom when messages are updated
    useEffect(() => {
        const current = messagesRef.current;
        if (!current) {
            return;
        }

        const top = current.scrollHeight - current.clientHeight - OFFSET_TO_SEE_USER_MESSAGE_IN_PX;
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

const OFFSET_TO_SEE_USER_MESSAGE_IN_PX = 60;

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
                <div style={{ height: "100px", width: "100%" }}>Loading...</div>
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

    const formattedThinkingTime = message.thinkingTimeInMs
        ? `${moment.duration(message.thinkingTimeInMs).asSeconds().toFixed(2)}s`
        : null;

    return (
        <div>
            {formattedThinkingTime && <div className="text-muted">Thought for {formattedThinkingTime}</div>}
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

        return <Code code={code} elementToCopy={code} language={language} className="mb-2" />;
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
        case "rql":
            return "sql";
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
