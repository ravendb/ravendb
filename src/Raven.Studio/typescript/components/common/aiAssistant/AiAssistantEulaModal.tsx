import Modal from "components/common/Modal";
import { Checkbox } from "../Checkbox";
import useBoolean from "components/hooks/useBoolean";
import { useAppDispatch } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import RichAlert from "../RichAlert";
import { aiAssistantActions } from "../shell/aiAssistantSlice";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "../ButtonWithSpinner";

interface AiAssistantEulaModalProps {
    close: () => void;
}

export function AiAssistantEulaModal({ close }: AiAssistantEulaModalProps) {
    const dispatch = useAppDispatch();

    const { value: isAccepted, toggle: toggleAccepted } = useBoolean(false);

    const asyncHandleConsent = useAsyncCallback(async () => {
        await dispatch(aiAssistantActions.giveConsent()).unwrap();
        close();
    });

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
                    <a href="https://ravendb.net/contact" target="_blank">
                        contact our support team
                    </a>
                    .
                </RichAlert>
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
                    I consent
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
