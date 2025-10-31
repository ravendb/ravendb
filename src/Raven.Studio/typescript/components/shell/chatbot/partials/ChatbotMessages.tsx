import { Icon } from "components/common/Icon";
import { chatbotActions, ChatbotMessage } from "../store/chatbotSlice";
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
import { useAppDispatch } from "components/store";

interface ChatbotMessagesProps {
    messages: ChatbotMessage[];
}

export default function ChatbotMessages({ messages }: ChatbotMessagesProps) {
    const messagesRef = useRef<HTMLDivElement>(null);

    // Scroll to the bottom when messages are updated
    useEffect(() => {
        if (!messagesRef.current) {
            return;
        }

        if (messagesRef.current) {
            messagesRef.current.scrollTo({
                top: messagesRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    return (
        <div ref={messagesRef} className="flex-grow-1 overflow-y-auto vstack gap-2">
            {messages.map((message) => (
                <div key={message.id} className="px-2">
                    <AiAgentMessage message={message} />
                </div>
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    message: ChatbotMessage;
}

function AiAgentMessage({ message }: AiAgentMessageProps) {
    switch (message.role) {
        case "user":
            return <UserMessage message={message} />;
        case "assistant":
            return <AgentMessage message={message} />;
        default:
            return assertUnreachable(message.role);
    }
}

interface UserMessageProps {
    message: ChatbotMessage;
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
    message: ChatbotMessage;
}

function AgentMessage({ message }: AgentMessageProps) {
    const dispatch = useAppDispatch();

    if (message.state === "loading") {
        return (
            <LazyLoad active>
                <div style={{ height: "100px", width: "100%" }}>Loading...</div>
            </LazyLoad>
        );
    }

    if (message.state === "error") {
        return (
            <RichAlert variant="danger">
                Failed to get response from AI Assistant.{" "}
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
            {formattedThinkingTime && (
                <div className="text-muted">
                    Though for {formattedThinkingTime}
                    <Icon icon="chevron-right" margin="ms-1" size="sm" />
                </div>
            )}
            <div className="mt-1">
                <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
                    {message.content}
                </ReactMarkdown>
            </div>
            {message.relevantLinks?.length > 0 && (
                <div className="hstack gap-1 flex-wrap">
                    {message.relevantLinks.map((link) => (
                        <a
                            key={link.Url}
                            href={link.Url}
                            target="_blank"
                            className="btn btn-sm rounded-pill p-1 panel-bg-2 border border-secondary text-reset"
                        >
                            <Icon icon="raven" size="sm" color="info" />
                            {link.Title}
                        </a>
                    ))}
                </div>
            )}
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
        let language: CodeLanguage = "plaintext";
        if (typeof node?.properties?.className === "string") {
            language = node.properties.className.replace("language-", "") as CodeLanguage;
        }

        // TODO fix typing
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-expect-error
        if (typeof node?.children?.[0]?.properties?.className?.[0] === "string") {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-expect-error
            language = node.children[0].properties.className[0].replace("language-", "") as CodeLanguage;
        }

        let code = "";
        if (isValidElement(children) && children.props && typeof (children.props as any).children === "string") {
            code = (children.props as any).children;
        } else if (typeof children === "string") {
            code = children;
        }

        language = supportedCodeLanguages.includes(language) ? language : "plaintext";

        return <Code code={code} elementToCopy={code} language={language} />;
    },
};
