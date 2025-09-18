import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { AssistAiAssistantRequestDto } from "commands/aiAssistant/assistAiAssistantCommand";
import { ReactNode } from "react";

interface AiAssistWindowProps {
    closeWindow: () => void;
    acceptResult: (text: string) => void;
    data: AssistAiAssistantRequestDto;
    successMessage: ReactNode;
}

export default function AiAssistantWindow({ closeWindow, data, acceptResult, successMessage }: AiAssistWindowProps) {
    const { aiAssistantService } = useServices();

    // Move it to redux
    const asyncCheckConsent = useAsync(aiAssistantService.checkConsent, []);
    const asyncGiveConsent = useAsyncCallback(aiAssistantService.giveConsent, {
        onSuccess: () => {
            asyncCheckConsent.execute();
        },
    });

    const hasConsent = asyncCheckConsent.status === "success" && asyncCheckConsent.result?.Status === "Success";
    const hasNoConsent = asyncCheckConsent.status === "success" && asyncCheckConsent.result?.Status !== "Success";

    const asyncAssist = useAsync(async () => {
        if (!hasConsent) {
            return null;
        }

        return await aiAssistantService.assist(data);
    }, [hasConsent]);

    const getAssistResultText = () => {
        if (!asyncAssist.result) {
            return null;
        }

        if (data.OperationType === "RefineGenAiPrompt") {
            return asyncAssist.result.RefinedPrompt;
        }

        if (data.OperationType === "RefineText") {
            return asyncAssist.result.RefinedText;
        }

        return null;
    };

    const assistResultText = getAssistResultText();

    const handleAccept = () => {
        acceptResult(assistResultText);
        closeWindow();
    };

    return (
        <div
            className="position-absolute p-2 border border-info rounded-2 bg-faded-community text-reset"
            style={{
                right: "10px",
                bottom: "10px",
                zIndex: 10,
                width: "500px",
                minHeight: "213px",
            }}
        >
            <div className="hstack justify-content-between align-items-center mb-2">
                <div>
                    <Icon icon="refine-ai" />
                    AI Assist Panel
                </div>
                <Button variant="link" className="text-reset" onClick={closeWindow} size="sm">
                    <Icon icon="close" margin="m-0" />
                </Button>
            </div>
            {asyncCheckConsent.loading && (
                <div className="hstack align-items-center gap-1">
                    <Spinner size="sm" variant="progress" />
                    Checking consent... Please wait.
                </div>
            )}
            {hasNoConsent && (
                <div>
                    To use our built-in AI features, such as AI Assistant, you need to provide consent. If you do not
                    accept, the feature will remain unavailable until you do.
                    {/* // TODO Eula */}
                    <div className="mt-2 justify-content-end">
                        <Button variant="primary" className="rounded-pill" onClick={asyncGiveConsent.execute}>
                            Review the consent
                        </Button>
                    </div>
                </div>
            )}
            {hasConsent && asyncAssist.loading && (
                <div className="hstack align-items-center gap-1">
                    <Spinner size="sm" variant="progress" />
                    Text refine in progress... Please wait.
                </div>
            )}
            {assistResultText && (
                <div>
                    <div className="mb-2">{successMessage}</div>
                    <Form.Control defaultValue={assistResultText} readOnly as="textarea" rows={3} className="mb-2" />
                    <div className="hstack gap-2 justify-content-end">
                        <Button variant="secondary" className="rounded-pill" onClick={closeWindow}>
                            <Icon icon="cancel" />
                            Discard
                        </Button>
                        <Button variant="primary" className="rounded-pill" onClick={handleAccept}>
                            <Icon icon="check" />
                            Accept
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
}
