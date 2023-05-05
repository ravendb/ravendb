import React, { useMemo } from "react";
import Prism from "prismjs";
import "./Code.scss";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");

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
    | "csharp";

interface CodeProps {
    code: string;
    language: Language;
}

export default function Code({ code, language }: CodeProps) {
    const html = useMemo(() => Prism.highlight(code, Prism.languages[language], language), [code, language]);

    return (
        <div className="code">
            <pre className="code-classes">
                <code className={`language-${language}`}>
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </code>
            </pre>
        </div>
    );
}
