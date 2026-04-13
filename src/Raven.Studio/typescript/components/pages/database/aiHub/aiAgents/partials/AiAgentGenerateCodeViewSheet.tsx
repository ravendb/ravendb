import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import { useState } from "react";
import ClickableCard from "components/common/ClickableCard";
import { LoadError } from "components/common/LoadError";
import Code, { CodeLanguage } from "components/common/Code";
import assertUnreachable from "components/utils/assertUnreachable";
import { AiAgentGenerateCodeLanguage } from "commands/database/aiAgents/generateCodeAiAgentCommand";
import { LazyLoad } from "components/common/LazyLoad";

interface AiAgentGenerateCodeViewSheetProps {
    agentId: string;
}

export default function AiAgentGenerateCodeViewSheet({ agentId }: AiAgentGenerateCodeViewSheetProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();
    const [language, setLanguage] = useState<AiAgentGenerateCodeLanguage>();

    const asyncGenerateCode = useAsync(async () => {
        if (!language) {
            return null;
        }

        const result = await aiAgentService.generateCode(databaseName, agentId, language);
        return result.GeneratedCode;
    }, [language]);

    return (
        <ViewSheet className="h-100 validation-schema-view-sheet-panel">
            <ViewSheet.Header>
                <h3 className="mb-0">
                    <Icon icon="magic-wand" color="primary" />
                    AI Agent client code
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="p-2 vstack gap-2">
                <span className="text-center text-uppercase">Select the language</span>
                <ClickableCard
                    icon="csharp"
                    title="C#"
                    onClick={() => setLanguage("c#")}
                    isSelected={language === "c#"}
                    className="panel-bg-2"
                />
                <ClickableCard
                    // TODO add python icon
                    icon="client"
                    title="Python"
                    onClick={() => setLanguage("python")}
                    isSelected={language === "python"}
                    className="panel-bg-2"
                />
                <ClickableCard
                    icon="javascript"
                    title="JavaScript"
                    onClick={() => setLanguage("javascript")}
                    isSelected={language === "javascript"}
                    className="panel-bg-2"
                />
                {asyncGenerateCode.loading && (
                    <LazyLoad active className="flex-grow-1">
                        <div className="h-100" />
                    </LazyLoad>
                )}
                {asyncGenerateCode.error && (
                    <LoadError error="Unable to generate code" refresh={asyncGenerateCode.execute} />
                )}
                {asyncGenerateCode.result && (
                    <Code language={getCodeComponentLanguage(language)} code={asyncGenerateCode.result} />
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
