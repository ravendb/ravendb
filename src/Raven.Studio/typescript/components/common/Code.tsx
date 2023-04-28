import React, { useEffect } from "react";
import { highlightAll } from "prismjs";
import "./Code.scss";

interface CodeProps {
    code: string;
    language: string;
}

export default function Code({ code, language }: CodeProps) {
    useEffect(() => {
        // does not work in Storybook
        highlightAll();
    }, []);

    return (
        <div className="code">
            <pre className="code-classes">
                <code className={`language-${language}`}>{code}</code>
            </pre>
        </div>
    );
}
