import "./Code.scss";
import { useMemo } from "react";
import Prism from "prismjs";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import queryCriteria from "models/database/query/queryCriteria";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import useConfirm from "components/common/ConfirmDialog";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");
require("prismjs/components/prism-json");
require("prismjs/components/prism-sql");
require("prismjs/components/prism-python");

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
    "python",
] as const;

export type CodeLanguage = (typeof supportedCodeLanguages)[number];

interface CodeProps {
    code: string;
    language: CodeLanguage;
    className?: string;
    codeClassName?: string;
    whiteSpace?: "pre" | "normal";
    isActionsHidden?: boolean;
    sourceView?: "chatbot";
    isTitleHidden?: boolean;
}

export default function Code(props: CodeProps) {
    const { code, className, codeClassName, whiteSpace, isActionsHidden, sourceView, isTitleHidden } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const chatbotDatabaseContext = useAppSelector((state) =>
        chatbotSelectors.attachedContextById(state, "DatabaseName")
    );
    const hasDatabase = !!activeDatabaseName || chatbotDatabaseContext?.state === "included";

    const languageTitle = languageTitles[props.language];
    const languageToHighlight = getLanguageToHighlight(props.language);

    const html = useMemo(
        () => Prism.highlight(code, Prism.languages[languageToHighlight], languageToHighlight),
        [code, languageToHighlight]
    );

    const confirm = useConfirm();

    const executeQuery = async (dbName: string) => {
        const query = queryCriteria.empty();
        query.queryText(code);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();

        savedQueriesStorage.saveAndNavigate(dbName, queryDto, {
            newWindow: false,
            extraParameters: sourceView === "chatbot" ? "&sourceView=chatbot" : "",
        });
    };

    const handleRunQuery = async () => {
        if (
            sourceView === "chatbot" &&
            chatbotDatabaseContext?.state === "included" &&
            chatbotDatabaseContext.value !== activeDatabaseName
        ) {
            const isConfirmed = await confirm({
                title: "Change active database",
                message: (
                    <div>
                        This query is intended for the <strong>{chatbotDatabaseContext.value}</strong> database.
                        <br />
                        <br />
                        Clicking &quot;Change and run&quot; will switch the active database to{" "}
                        <strong>{chatbotDatabaseContext.value}</strong> and execute the query.
                    </div>
                ),
                confirmText: "Change and run",
                confirmIcon: "rocket",
            });

            if (!isConfirmed) {
                return;
            }

            executeQuery(chatbotDatabaseContext.value);
        } else {
            executeQuery(activeDatabaseName);
        }
    };

    return (
        <div className={classNames("code", className)}>
            {!isActionsHidden && (
                <div className="code-actions">
                    {!isTitleHidden && languageTitle && <div>{languageTitle}</div>}
                    <div className="hstack gap-2 ms-auto">
                        <Button
                            variant="link"
                            className="text-emphasis"
                            title="Copy to clipboard"
                            onClick={() => copyToClipboard.copy(`${code}`, `Copied to clipboard`)}
                        >
                            <Icon icon="copy" />
                            Copy
                        </Button>
                        {hasDatabase && props.language === "rql" && (
                            <Button variant="link" className="text-emphasis" onClick={handleRunQuery}>
                                <Icon icon="rocket" />
                                Run query
                            </Button>
                        )}
                    </div>
                </div>
            )}
            <pre className="code-classes d-flex flex-grow-1 m-0">
                <code
                    className={classNames(`language-${languageToHighlight}`, "monospace-font", codeClassName)}
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
    python: "Python",
};
