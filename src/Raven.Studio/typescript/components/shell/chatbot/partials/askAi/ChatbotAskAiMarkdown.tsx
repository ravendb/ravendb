import Code, { CodeLanguage, supportedCodeLanguages } from "components/common/Code";
import { isValidElement } from "react";
import Table from "react-bootstrap/Table";
import ReactMarkdown, { Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import { Element } from "hast";

interface ChatbotAskAiMarkdownProps {
    content: string;
}

export default function ChatbotAskAiMarkdown({ content }: ChatbotAskAiMarkdownProps) {
    return (
        <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
            {content}
        </ReactMarkdown>
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
        <Table bordered striped hover responsive className="mb-2">
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

        return <Code code={code} language={language} className="mb-2" sourceView="chatbot" />;
    },
    li: ({ children }) => <li className="word-break">{children}</li>,
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
