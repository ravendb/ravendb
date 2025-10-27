import useBoolean from "components/hooks/useBoolean";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import RichAlert from "../RichAlert";
import { aiAssistantActions, aiAssistantSelectors } from "../shell/aiAssistantSlice";
import { aiAssistantConstants } from "./aiAssistantConstants";
import { AiAssistantEulaModal } from "./AiAssistantEulaModal";

interface AiAssistantConsentStatusCheckerProps {
    className?: string;
}

export default function AiAssistantConsentStatusChecker({ className }: AiAssistantConsentStatusCheckerProps) {
    const dispatch = useAppDispatch();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const { value: isEulaOpen, toggle: toggleEulaOpen } = useBoolean(false);

    const isConsentSuccess = consentStatus.data === "Success";

    return (
        <>
            {!isConsentSuccess && (
                <div className={className}>
                    {consentStatus.status === "loading" && (
                        <div className="hstack align-items-center gap-1">
                            <Spinner size="sm" variant="progress" />
                            Checking consent... Please wait.
                        </div>
                    )}
                    {consentStatus.status === "failure" && (
                        <RichAlert variant="danger">
                            Failed to check consent.{" "}
                            <Button
                                variant="link"
                                className="px-0"
                                onClick={() => dispatch(aiAssistantActions.checkConsent())}
                            >
                                Please try again
                            </Button>
                        </RichAlert>
                    )}
                    {consentStatus.data === "InvalidCredentials" && (
                        <RichAlert variant="danger">{aiAssistantConstants.invalidCredentials}</RichAlert>
                    )}
                    {consentStatus.data === "ConsentRequired" && (
                        <div>
                            To use our built-in AI features, such as <i>AI Assistant</i>, you need to provide consent.
                            If you do not accept, the feature will remain unavailable until you do.
                            <div className="hstack justify-content-end mt-2">
                                <Button variant="primary" className="rounded-pill" onClick={toggleEulaOpen}>
                                    Review the consent
                                    <Icon icon="open-modal" margin="ms-1" />
                                </Button>
                            </div>
                        </div>
                    )}
                </div>
            )}
            {isEulaOpen && <AiAssistantEulaModal close={toggleEulaOpen} />}
        </>
    );
}
