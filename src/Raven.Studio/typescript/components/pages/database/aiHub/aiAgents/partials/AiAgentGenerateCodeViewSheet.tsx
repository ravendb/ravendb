import "./AiAgentGenerateCodeViewSheet.scss";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import { useState } from "react";
import { LoadError } from "components/common/LoadError";
import Code, { CodeLanguage } from "components/common/Code";
import assertUnreachable from "components/utils/assertUnreachable";
import { AiAgentGenerateCodeLanguage } from "commands/database/aiAgents/generateCodeAiAgentCommand";
import Nav from "react-bootstrap/Nav";
import { LazyLoad } from "components/common/LazyLoad";

interface AiAgentGenerateCodeViewSheetProps {
    agentId: string;
}

export default function AiAgentGenerateCodeViewSheet({ agentId }: AiAgentGenerateCodeViewSheetProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();
    const [language, setLanguage] = useState<AiAgentGenerateCodeLanguage>("c#");

    const asyncGenerateCode = useAsync(async () => {
        if (!databaseName) {
            return;
        }

        const result = await aiAgentService.generateCode(databaseName, agentId, language);
        return result.GeneratedCode;
    }, [language, agentId, databaseName]);

    return (
        <ViewSheet className="h-100 ai-agent-generate-code-view-sheet">
            <ViewSheet.Header>
                <h3 className="mb-0">
                    <Icon icon="magic-wand" color="primary" />
                    AI Agent client code
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="px-0 vstack view-sheet-body">
                <Nav justify variant="tabs" activeKey={language}>
                    <Nav.Item>
                        <Nav.Link
                            active={language === "c#"}
                            onClick={() => setLanguage("c#")}
                            className="no-decor text-reset"
                        >
                            <Icon icon="csharp" />
                            C#
                        </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                        <Nav.Link
                            active={language === "python"}
                            onClick={() => setLanguage("python")}
                            className="no-decor text-reset"
                        >
                            <Icon icon="python" />
                            Python
                        </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                        <Nav.Link
                            active={language === "javascript"}
                            onClick={() => setLanguage("javascript")}
                            className="no-decor text-reset"
                        >
                            <Icon icon="javascript" />
                            JavaScript
                        </Nav.Link>
                    </Nav.Item>
                </Nav>
                {asyncGenerateCode.loading && (
                    <LazyLoad active className="flex-grow-1">
                        <div className="h-100" />
                    </LazyLoad>
                )}
                {asyncGenerateCode.error && (
                    <LoadError error="Unable to generate code" refresh={asyncGenerateCode.execute} />
                )}
                {asyncGenerateCode.result && (
                    <Code
                        language={getCodeComponentLanguage(language)}
                        code={asyncGenerateCode.result}
                        className="rounded-top-0 border border-top-0"
                        isTitleHidden
                    />
                )}
            </ViewSheet.Body>
        </ViewSheet>
    );
}

function getCodeComponentLanguage(language: AiAgentGenerateCodeLanguage): CodeLanguage {
    switch (language) {
        case "c#":
            return "csharp";
        case "python":
            return "python";
        case "javascript":
            return "javascript";
        default:
            assertUnreachable(language);
    }
}
