import React, { useMemo } from "react";
import Prism from "prismjs";
import "./Code.scss";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import { Button } from "reactstrap";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");
require("prismjs/components/prism-json");
require("prismjs/components/prism-json");

type Language =
    | "plaintext"
    | "markup"
    | "html"
    | "mathml"
    | "svg"
    | "xml"
    | "ssml"
    | "atom"
    | "rss"
    | "css"
    | "clike"
    | "javascript"
    | "csharp"
    | "json"
    | "csharp"
    | "json";

interface CodeProps {
    code: string;
    language: Language;
    className?: string;
    elementToCopy?: string;
}

export default function Code({ code, language, className, elementToCopy }: CodeProps) {
    const html = useMemo(() => Prism.highlight(code, Prism.languages[language], language), [code, language]);

    return (
        <div className={classNames("code d-flex flex-grow-1 position-relative", className)}>
            {elementToCopy && (
                <Button
                    className="rounded-pill position-absolute end-gutter-xs top-gutter-xs"
                    size="xs"
                    title="Copy to clipboard"
                    onClick={() => copyToClipboard.copy(`${elementToCopy}`, `Copied to clipboard`)}
                >
                    <Icon icon="copy" margin="m-0" />
                </Button>
            )}
            <pre className="code-classes d-flex flex-grow-1 m-0">
                <code className={`language-${language}`}>
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </code>
            </pre>
        </div>
    );
}
