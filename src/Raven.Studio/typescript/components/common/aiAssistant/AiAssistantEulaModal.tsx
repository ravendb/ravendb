import Modal from "components/common/Modal";
import { Checkbox } from "../Checkbox";
import useBoolean from "components/hooks/useBoolean";
import { useAppDispatch } from "components/store";
import Button from "react-bootstrap/Button";
import { aiAssistantActions } from "../shell/aiAssistantSlice";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "../ButtonWithSpinner";
const aiAssistantImg = require("Content/img/ai-assistant-terms-of-use.webp");

interface AiAssistantEulaModalProps {
    close: () => void;
    onConsentGiven?: () => void;
}

export function AiAssistantEulaModal({ close, onConsentGiven }: AiAssistantEulaModalProps) {
    const dispatch = useAppDispatch();

    const { value: isAccepted, toggle: toggleAccepted } = useBoolean(false);

    const asyncHandleConsent = useAsyncCallback(async () => {
        const result = await dispatch(aiAssistantActions.giveConsent()).unwrap();
        if (result === "Success") {
            onConsentGiven?.();
            close();
        }
    });

    return (
        <Modal show onHide={close} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={close} className="pb-0">
                <h3>Get Started with AI Assistant</h3>
            </Modal.Header>
            <div className="px-4 rounded overflow-hidden">
                <img src={aiAssistantImg} className="w-100 rounded-2" />
            </div>
            <Modal.Body className="py-1 vstack gap-2">
                <div className="mt-2">
                    The built-in AI Assistant is designed to supercharge your workflow. To enable this feature, please
                    review and accept the Terms of Use.
                </div>
                <Checkbox selected={isAccepted} toggleSelection={toggleAccepted} color="primary">
                    I accept the{" "}
                    <a href="https://ravendb.net/legal/ravendb/ai-assistant-terms-of-use" target="_blank">
                        RavenDB AI Assistant Terms of Use
                    </a>
                </Checkbox>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={close} className="link-muted rounded-pill">
                    Cancel
                </Button>
                <ButtonWithSpinner
                    variant="primary"
                    onClick={asyncHandleConsent.execute}
                    className="rounded-pill"
                    disabled={!isAccepted}
                    isSpinning={asyncHandleConsent.loading}
                >
                    Agree & Enable
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
