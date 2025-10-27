import "./AiAssistantWindow.scss";
import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import { RefinePromptAiAssistantViewData } from "commands/aiAssistant/refinePromptAiAssistantCommand";
import { ReactNode } from "react";
import { useAppSelector } from "components/store";
import { aiAssistantSelectors } from "../shell/aiAssistantSlice";
import RichAlert from "../RichAlert";
import { aiAssistantConstants } from "./aiAssistantConstants";
import AiAssistantConsentStatusChecker from "./AiAssistantConsentStatusChecker";

interface AiAssistWindowProps {
    closeWindow: () => void;
    acceptResult: (text: string) => void;
    data: RefinePromptAiAssistantViewData;
    successMessage: ReactNode;
}

export default function AiAssistantWindow({ closeWindow, data, acceptResult, successMessage }: AiAssistWindowProps) {
    const { aiAssistantService } = useServices();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);

    const isConsentSuccess = consentStatus.data === "Success";

    const asyncAssist = useAsync(async () => {
        if (!isConsentSuccess) {
            return null;
        }

        return await aiAssistantService.refinePrompt(data);
    }, [isConsentSuccess]);

    const getAssistResultText = () => {
        if (!asyncAssist.result || asyncAssist.result.Status !== "Success") {
            return null;
        }

        return asyncAssist.result.RefinedPrompt;
    };

    const assistResultText = getAssistResultText();

    const handleAccept = () => {
        acceptResult(assistResultText);
        closeWindow();
    };

    return (
        <div
            className="ai-assistant-window position-absolute rounded-2 text-reset"
            style={{
                right: "10px",
                bottom: "10px",
                zIndex: 10,
                width: "500px",
            }}
        >
            <div className="ai-assistant-window-inner p-2 rounded-2">
                <div className="hstack justify-content-between align-items-center mb-2">
                    <div>
                        <Icon icon="refine-ai" />
                        AI Assistant
                    </div>
                    <Button variant="link" className="text-reset" onClick={closeWindow} size="sm">
                        <Icon icon="close" margin="m-0" />
                    </Button>
                </div>
                <AiAssistantConsentStatusChecker />
                {isConsentSuccess && asyncAssist.loading && (
                    <div className="hstack align-items-center gap-1">
                        <Spinner size="sm" variant="progress" />
                        Text refine in progress... Please wait.
                    </div>
                )}
                {asyncAssist.error && <RichAlert variant="danger">Failed to assist. Please try again.</RichAlert>}
                {asyncAssist.result?.Status === "InvalidData" && (
                    <RichAlert variant="danger">Invalid data. Please check your data and try again.</RichAlert>
                )}
                {asyncAssist.result?.Status === "OutOfTokens" && (
                    <RichAlert variant="danger">
                        You have used all your AI Assistant tokens for this month. Your token allowance will be reset at
                        the beginning of the next month.
                    </RichAlert>
                )}
                {asyncAssist.result?.Status === "InvalidCredentials" && (
                    <RichAlert variant="danger">{aiAssistantConstants.invalidCredentials}</RichAlert>
                )}
                {assistResultText && (
                    <div>
                        <div className="mb-2">{successMessage}</div>
                        <Form.Control
                            defaultValue={assistResultText}
                            readOnly
                            as="textarea"
                            rows={3}
                            className="mb-2"
                        />
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
        </div>
    );
}
