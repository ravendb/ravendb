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
import { chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import useConfirm from "components/common/ConfirmDialog";
import databasesManager from "common/shell/databasesManager";
import Dropdown from "react-bootstrap/Dropdown";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { CustomDropdownToggle } from "components/common/Dropdown";

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
    sourceView?: "chatbot";
}

export default function Code(props: CodeProps) {
    const { code, className, codeClassName, whiteSpace, isActionsHidden, sourceView } = props;

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

    const executeQuery = (isDisableAutoIndexCreation: boolean) => {
        const query = queryCriteria.empty();
        query.queryText(code);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();

        let extraParameters = "";

        if (sourceView === "chatbot") {
            extraParameters += "&sourceView=chatbot";
        }
        if (isDisableAutoIndexCreation) {
            extraParameters += `&isDisableAutoIndexCreation=true`;
        }

        savedQueriesStorage.saveAndNavigate(activeDatabaseName, queryDto, {
            newWindow: false,
            extraParameters,
        });
    };

    const handleRunQuery = async (isDisableAutoIndexCreation: boolean) => {
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

            const db = databasesManager.default.getDatabaseByName(chatbotDatabaseContext.value);
            databasesManager.default.activate(db).then(() => {
                executeQuery(isDisableAutoIndexCreation);
            });
        } else {
            executeQuery(isDisableAutoIndexCreation);
        }
    };

    return (
        <div className={classNames("code", className)}>
            {!isActionsHidden && (
                <div className="code-actions">
                    {languageTitle && <div className="fs-6">{languageTitle}</div>}
                    <div className="hstack">
                        <Button
                            variant="link"
                            className="text-emphasis fs-6"
                            title="Copy to clipboard"
                            onClick={() => copyToClipboard.copy(`${code}`, `Copied to clipboard`)}
                        >
                            <Icon icon="copy" />
                            Copy
                        </Button>
                        {hasDatabase && props.language === "rql" && (
                            <Dropdown as={ButtonGroup}>
                                <Button
                                    variant="link"
                                    className="text-emphasis fs-6 border-0 border-end border-secondary"
                                    onClick={() => handleRunQuery(false)}
                                >
                                    <Icon icon="rocket" />
                                    Run query
                                </Button>
                                <Dropdown.Toggle
                                    as={CustomDropdownToggle}
                                    isCaretHidden
                                    split
                                    variant="link"
                                    className="text-emphasis p-0 ps-1 fs-5"
                                >
                                    <Icon icon="chevron-down" margin="m-0" className="fs-5" />
                                </Dropdown.Toggle>
                                <Dropdown.Menu>
                                    <Dropdown.Item onClick={() => handleRunQuery(true)} className="fs-5">
                                        <Icon icon="query" />
                                        Run query without creating auto-index
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
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
