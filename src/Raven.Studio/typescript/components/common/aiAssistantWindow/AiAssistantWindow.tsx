import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import { AssistAiAssistantRequestDto } from "commands/aiAssistant/assistAiAssistantCommand";
import { ReactNode, useEffect } from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { aiAssistantActions, aiAssistantSelectors } from "../shell/aiAssistantSlice";
import useBoolean from "components/hooks/useBoolean";
import Modal from "components/common/Modal";
import { Checkbox } from "../Checkbox";
import RichAlert from "../RichAlert";

interface AiAssistWindowProps {
    closeWindow: () => void;
    acceptResult: (text: string) => void;
    data: AssistAiAssistantRequestDto;
    successMessage: ReactNode;
}

export default function AiAssistantWindow({ closeWindow, data, acceptResult, successMessage }: AiAssistWindowProps) {
    const dispatch = useAppDispatch();
    const { aiAssistantService } = useServices();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const { value: isEulaOpen, toggle: toggleEulaOpen } = useBoolean(false);

    const isConsentSuccess = consentStatus.data === "Success";

    // Check consent status if not checked yet
    useEffect(() => {
        if (consentStatus.status === "idle") {
            dispatch(aiAssistantActions.checkConsent());
        }
    }, [consentStatus.status]);

    const asyncAssist = useAsync(async () => {
        if (!isConsentSuccess) {
            return null;
        }

        return await aiAssistantService.assist(data);
    }, [isConsentSuccess]);

    const getAssistResultText = () => {
        if (!asyncAssist.result || asyncAssist.result.Status !== "Success") {
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
            }}
        >
            <div className="hstack justify-content-between align-items-center mb-2">
                <div>
                    <Icon icon="refine-ai" />
                    AI Assistant
                </div>
                <Button variant="link" className="text-reset" onClick={closeWindow} size="sm">
                    <Icon icon="close" margin="m-0" />
                </Button>
            </div>
            {consentStatus.status === "loading" && (
                <div className="hstack align-items-center gap-1">
                    <Spinner size="sm" variant="progress" />
                    Checking consent... Please wait.
                </div>
            )}
            {consentStatus.data === "InvalidCredentials" && (
                <RichAlert variant="danger">
                    Invalid credentials. Please check your credentials and try again.
                </RichAlert>
            )}
            {consentStatus.data === "ConsentRequired" && (
                <div>
                    To use our built-in AI features, such as <i>AI Assistant</i>, you need to provide consent. If you do
                    not accept, the feature will remain unavailable until you do.
                    <div className="hstack justify-content-end mt-2">
                        <Button variant="primary" className="rounded-pill" onClick={toggleEulaOpen}>
                            Review the consent
                            <Icon icon="open-modal" margin="ms-1" />
                        </Button>
                    </div>
                    {isEulaOpen && <EulaModal close={toggleEulaOpen} />}
                </div>
            )}
            {isConsentSuccess && asyncAssist.loading && (
                <div className="hstack align-items-center gap-1">
                    <Spinner size="sm" variant="progress" />
                    Text refine in progress... Please wait.
                </div>
            )}
            {asyncAssist.result?.Status === "InvalidData" && (
                <RichAlert variant="danger">Invalid data. Please check your data and try again.</RichAlert>
            )}
            {asyncAssist.result?.Status === "OutOfTokens" && (
                <RichAlert variant="danger">Out of tokens. TODO: some link to the pricing page.</RichAlert>
            )}
            {asyncAssist.result?.Status === "InvalidCredentials" && (
                <RichAlert variant="danger">
                    Invalid credentials. Please check your credentials and try again.
                </RichAlert>
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

interface EulaModalProps {
    close: () => void;
}

function EulaModal({ close }: EulaModalProps) {
    const dispatch = useAppDispatch();

    const { value: isAccepted, toggle: toggleAccepted } = useBoolean(false);

    const handleConsent = () => {
        dispatch(aiAssistantActions.giveConsent());
        close();
    };

    return (
        <Modal show onHide={close} contentClassName="modal-border bulge-primary" size="lg">
            <Modal.Header closeButton onCloseClick={close} className="pb-0">
                <h3>
                    <Icon icon="license" />
                    Review the consent
                </h3>
            </Modal.Header>
            <Modal.Body className="py-1 vstack gap-3">
                <div>
                    To use our built-in AI features, such as <i>AI Assistant</i>, you need to provide consent. If you do
                    not accept, the feature will remain unavailable until you do.
                </div>
                <Checkbox selected={isAccepted} toggleSelection={toggleAccepted} color="primary">
                    I accept the{" "}
                    <a href="#TODO" target="_blank">
                        RavenDB AI Assistant EULA
                    </a>
                </Checkbox>
                <RichAlert variant="info">
                    If you wish to revert your consent,{" "}
                    <a href="#TODO" target="_blank">
                        contact our support team
                    </a>
                    .
                </RichAlert>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={close} className="link-muted rounded-pill">
                    Cancel
                </Button>
                <Button variant="primary" onClick={handleConsent} className="rounded-pill" disabled={!isAccepted}>
                    I consent
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
