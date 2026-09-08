import type { Element, ElementContent } from "hast";
import Markdown, { type Components } from "react-markdown";
import rehypeSanitize from "rehype-sanitize";
import remarkGfm from "remark-gfm";
import remend from "remend";
import { CodeBlock } from "@/components/code-block";

const REMARK_PLUGINS = [remarkGfm];
const REHYPE_PLUGINS = [rehypeSanitize];

function nodeText(node: ElementContent | undefined): string {
    if (node === undefined) return "";
    if (node.type === "text") return node.value;
    if (node.type === "element") return node.children.map(nodeText).join("");
    return "";
}

function languageOf(node: Element): string | null {
    const className = node.properties?.className;
    const classes = Array.isArray(className) ? className : typeof className === "string" ? [className] : [];
    for (const candidate of classes) {
        const match = /^language-(.+)$/.exec(String(candidate));
        if (match !== null) return match[1];
    }
    return null;
}

// Styling lives here rather than in a global stylesheet so the widget's markdown can't leak rules onto
// anything else, and so `rehype-sanitize` stays free to drop attributes without breaking the look.
const COMPONENTS: Components = {
    p: ({ children }) => <p className="my-2 first:mt-0 last:mb-0">{children}</p>,
    a: ({ href, children }) => (
        <a
            href={href}
            target="_blank"
            rel="noopener noreferrer"
            className="text-rq-accent underline underline-offset-2 hover:no-underline"
        >
            {children}
        </a>
    ),
    strong: ({ children }) => <strong className="font-semibold">{children}</strong>,
    em: ({ children }) => <em className="italic">{children}</em>,
    ul: ({ children }) => <ul className="my-2 list-disc space-y-1 ps-5 first:mt-0 last:mb-0">{children}</ul>,
    ol: ({ children }) => <ol className="my-2 list-decimal space-y-1 ps-5 first:mt-0 last:mb-0">{children}</ol>,
    li: ({ children }) => <li className="[&>ol]:my-1 [&>ul]:my-1">{children}</li>,
    h1: ({ children }) => <h1 className="mt-4 mb-2 text-base font-semibold first:mt-0">{children}</h1>,
    h2: ({ children }) => <h2 className="mt-4 mb-2 text-base font-semibold first:mt-0">{children}</h2>,
    h3: ({ children }) => <h3 className="mt-3 mb-1.5 text-sm font-semibold first:mt-0">{children}</h3>,
    h4: ({ children }) => <h4 className="mt-3 mb-1.5 text-sm font-semibold first:mt-0">{children}</h4>,
    blockquote: ({ children }) => (
        <blockquote className="border-rq-border text-rq-muted my-2 border-s-2 ps-3">{children}</blockquote>
    ),
    hr: () => <hr className="border-rq-border my-3" />,
    code: ({ children }) => (
        <code className="rounded-rq-sm bg-rq-code px-1 py-0.5 font-mono text-[0.9em]">{children}</code>
    ),
    pre: ({ node }) => {
        const codeNode = node?.children?.[0];
        const isCode = codeNode?.type === "element" && codeNode.tagName === "code";
        if (isCode === false) return null;

        const code = nodeText(codeNode).replace(/\n$/, "");
        return <CodeBlock code={code} language={languageOf(codeNode as Element)} />;
    },
    table: ({ children }) => (
        <div className="rounded-rq-sm border-rq-border my-3 overflow-x-auto border">
            <table className="w-full border-collapse text-start text-[13px]">{children}</table>
        </div>
    ),
    thead: ({ children }) => <thead className="bg-rq-surface">{children}</thead>,
    tr: ({ children }) => <tr className="border-rq-border border-b last:border-0">{children}</tr>,
    th: ({ children }) => <th className="px-3 py-2 text-start font-semibold">{children}</th>,
    td: ({ children }) => <td className="px-3 py-2 align-top">{children}</td>,
};

type MarkdownMessageProps = {
    content: string;
    /** While a turn streams the buffer is half-written, so `remend` closes dangling emphasis, fences and
     *  table rows for this render only. A finished message is already well-formed and skips the repair. */
    isStreaming?: boolean;
};

export function MarkdownMessage({ content, isStreaming = false }: MarkdownMessageProps) {
    return (
        <Markdown remarkPlugins={REMARK_PLUGINS} rehypePlugins={REHYPE_PLUGINS} components={COMPONENTS}>
            {isStreaming ? remend(content) : content}
        </Markdown>
    );
}
