import "./AiAssistantWindow.scss";
import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import {
    RefinePromptAiAssistantResultDto,
    RefinePromptAiAssistantViewData,
} from "commands/aiAssistant/refinePromptAiAssistantCommand";
import { ReactNode, useMemo, useState } from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { aiAssistantActions, aiAssistantSelectors } from "../shell/aiAssistantSlice";
import RichAlert from "../RichAlert";
import { aiAssistantConstants } from "./aiAssistantConstants";
import AiAssistantConsentStatusChecker from "./AiAssistantConsentStatusChecker";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createLoadingState, createSuccessState } from "components/utils/common";
import { processStreamingResponse } from "components/utils/aiAssistStreamingUtils";
import ButtonWithSpinner from "../ButtonWithSpinner";
import useTypewriter from "components/hooks/useTypewriter";

interface AiAssistWindowProps {
    closeWindow: () => void;
    acceptResult: (text: string) => void;
    data: RefinePromptAiAssistantViewData;
    successMessage: ReactNode;
    right?: string;
    bottom?: string;
}

export default function AiAssistantWindow({
    closeWindow,
    data,
    acceptResult,
    successMessage,
    right = "14px",
    bottom = "14px",
}: AiAssistWindowProps) {
    const dispatch = useAppDispatch();
    const { aiAssistantService } = useServices();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const isConsentSuccess = consentStatus.data === "Success";

    const [assistResult, setAssistResult] = useState<loadableData<RefinePromptAiAssistantResultDto>>(createIdleState());
    const abortController = useMemo(() => new AbortController(), []);

    const asyncAssist = useAsync(async () => {
        if (!isConsentSuccess) {
            return;
        }

        setAssistResult(createLoadingState());

        const result = await processStreamingResponse<RefinePromptAiAssistantResultDto>({
            promiseFn: () => aiAssistantService.refinePrompt(data, abortController.signal),
            streamPropertyPath: "RefinedPrompt",
            onChunksCombined: (text) => {
                setAssistResult((prev) => {
                    if (prev.data) {
                        return createSuccessState({
                            ...prev.data,
                            RefinedPrompt: text,
                        });
                    } else {
                        return createSuccessState({
                            RefinedPrompt: text,
                            Status: "Success",
                            UsagePercentage: 0,
                        });
                    }
                });
            },
            abortSignal: abortController.signal,
        });

        if (result.status === "Error") {
            setAssistResult(createFailureState(result.error));
            return;
        }

        if (result.status === "Success") {
            dispatch(aiAssistantActions.usagePercentageSet(result.data.UsagePercentage));
            setAssistResult(createSuccessState(result.data));
            return;
        }

        setAssistResult(createSuccessState({ Status: result.status, UsagePercentage: 0, RefinedPrompt: "" }));
    }, []);

    const refinedPromptTypewriter = useTypewriter({
        text: assistResult.data?.RefinedPrompt,
        isDone: asyncAssist.status === "success",
    });

    const handleAccept = () => {
        acceptResult(assistResult.data?.RefinedPrompt || "");
        closeWindow();
    };

    const handleClose = () => {
        abortController.abort();
        closeWindow();
    };

    return (
        <div
            className="ai-assistant-window position-absolute rounded-2 text-reset"
            style={{
                right,
                bottom,
                zIndex: 10,
                width: "500px",
            }}
        >
            <div className="ai-assistant-window-inner p-2 rounded-2">
                <div className="hstack justify-content-between align-items-center mb-2">
                    <div>
                        <Icon icon="ask-ai" />
                        AI Assistant
                    </div>
                    <Button variant="link" className="text-reset" onClick={handleClose} size="sm">
                        <Icon icon="close" margin="m-0" />
                    </Button>
                </div>
                <AiAssistantConsentStatusChecker onConsentGiven={asyncAssist.execute} />
                {isConsentSuccess && assistResult.status === "loading" && (
                    <div className="hstack align-items-center gap-1">
                        <Spinner size="sm" variant="progress" />
                        Text refine in progress... Please wait.
                    </div>
                )}
                {assistResult.error && <RichAlert variant="danger">Failed to assist. Please try again.</RichAlert>}
                <AiAssistStatus status={assistResult.data?.Status} />
                {assistResult.data?.Status === "Success" && (
                    <div>
                        <Form.Control
                            value={refinedPromptTypewriter}
                            readOnly
                            as="textarea"
                            className="refined-prompt-textarea"
                        />
                        <small className="text-muted">{successMessage}</small>
                        <div className="hstack gap-2 justify-content-end">
                            <Button variant="secondary" className="rounded-pill" onClick={handleClose}>
                                <Icon icon="cancel" />
                                Discard
                            </Button>
                            <ButtonWithSpinner
                                variant="primary"
                                className="rounded-pill"
                                onClick={handleAccept}
                                isSpinning={asyncAssist.loading}
                                icon="check"
                            >
                                Accept
                            </ButtonWithSpinner>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

interface AiAssistStatusProps {
    status: AiAssistantResponseStatus;
}

function AiAssistStatus({ status }: AiAssistStatusProps) {
    if (status === "InvalidData") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidData}</RichAlert>;
    }

    if (status === "OutOfTokens") {
        return <RichAlert variant="danger">{aiAssistantConstants.outOfTokens}</RichAlert>;
    }

    if (status === "InvalidCredentials") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidCredentials}</RichAlert>;
    }

    return null;
}
