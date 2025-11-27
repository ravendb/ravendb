import React, { useMemo } from "react";
import Prism from "prismjs";
import "./Code.scss";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import queryCriteria from "models/database/query/queryCriteria";
import savedQueriesStorage from "common/storage/savedQueriesStorage";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");
require("prismjs/components/prism-json");
require("prismjs/components/prism-sql");

export const supportedCodeLanguages = [
    "plaintext",
    "markup",
    "html",
    "mathml",
    "svg",
    "xml",
    "ssml",
    "atom",
    "rss",
    "css",
    "clike",
    "javascript",
    "csharp",
    "json",
    "sql",
    "rql",
] as const;

export type CodeLanguage = (typeof supportedCodeLanguages)[number];

interface CodeProps {
    code: string;
    language: CodeLanguage;
    className?: string;
    codeClassName?: string;
    whiteSpace?: "pre" | "normal";
    isActionsHidden?: boolean;
}

export default function Code(props: CodeProps) {
    const { code, className, codeClassName, whiteSpace, isActionsHidden } = props;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const languageTitle = languageTitles[props.language];
    const languageToHighlight = getLanguageToHighlight(props.language);

    const html = useMemo(
        () => Prism.highlight(code, Prism.languages[languageToHighlight], languageToHighlight),
        [code, languageToHighlight]
    );

    const handleRunQuery = () => {
        const query = queryCriteria.empty();

        query.queryText(code);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(databaseName, queryDto, {
            newWindow: false,
        });
    };

    return (
        <div className={classNames("code", className)}>
            {!isActionsHidden && (
                <div className="code-actions">
                    {languageTitle && <div className="fs-6">{languageTitle}</div>}
                    <div className="hstack gap-1">
                        <Button
                            variant="link"
                            className="text-emphasis fs-6"
                            title="Copy to clipboard"
                            onClick={() => copyToClipboard.copy(`${code}`, `Copied to clipboard`)}
                        >
                            <Icon icon="copy" />
                            Copy
                        </Button>
                        {props.language === "rql" && (
                            <Button
                                variant="link"
                                className="text-emphasis fs-6"
                                title="Copy to clipboard"
                                onClick={handleRunQuery}
                            >
                                <Icon icon="rocket" />
                                Run query
                            </Button>
                        )}
                    </div>
                </div>
            )}
            <pre className="code-classes d-flex flex-grow-1 m-0">
                <code
                    className={classNames(`language-${languageToHighlight}`, codeClassName)}
                    style={{ whiteSpace: whiteSpace ?? "pre" }}
                >
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </code>
            </pre>
        </div>
    );
}

function getLanguageToHighlight(language: CodeLanguage): CodeLanguage {
    switch (language) {
        case "rql":
            return "sql";
        default:
            return language;
    }
}

const languageTitles: Record<CodeLanguage, string> = {
    plaintext: "Plaintext",
    markup: "Markup",
    html: "HTML",
    mathml: "MathML",
    svg: "SVG",
    xml: "XML",
    ssml: "SSML",
    atom: "Atom",
    rss: "RSS",
    css: "CSS",
    clike: "C-like",
    javascript: "JavaScript",
    csharp: "C#",
    json: "JSON",
    sql: "SQL",
    rql: "RQL",
};
