import useBoolean from "components/hooks/useBoolean";
import { useAppDispatch, useAppSelector } from "components/store";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import RichAlert from "../RichAlert";
import { aiAssistantActions, aiAssistantSelectors } from "../shell/aiAssistantSlice";
import { aiAssistantConstants } from "./aiAssistantConstants";
import { AiAssistantEulaModal } from "./AiAssistantEulaModal";
import IconAsciiPlaceholder from "components/shell/chatbot/partials/askAi/iconAscii/IconAsciiPlaceholder";
import classNames from "classnames";

interface AiAssistantConsentStatusCheckerProps {
    className?: string;
    onConsentGiven?: () => void;
    hasAsciiIcon?: boolean;
}

export default function AiAssistantConsentStatusChecker({
    className,
    onConsentGiven,
    hasAsciiIcon = false,
}: AiAssistantConsentStatusCheckerProps) {
    const dispatch = useAppDispatch();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const { value: isEulaOpen, toggle: toggleEulaOpen } = useBoolean(false);

    const isConsentSuccess = consentStatus.data === "Success";

    return (
        <>
            {!isConsentSuccess && (
                <div className={className}>
                    {consentStatus.status === "loading" && (
                        <div className="hstack align-items-center gap-1 justify-content-center">
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
                        <div className={classNames(hasAsciiIcon && "vstack justify-content-center p-4 text-center")}>
                            {hasAsciiIcon && <IconAsciiPlaceholder />}
                            <p className={classNames("mb-0", hasAsciiIcon && "mt-2")}>
                                To use our built-in AI features, such as <em>AI Assistant</em>, you need to provide
                                consent. The feature will remain unavailable until accepted.
                            </p>
                            <div className={classNames(hasAsciiIcon ? "mt-3 justify-content-center" : "hstack mt-2")}>
                                <Button variant="primary" className="rounded-pill" onClick={toggleEulaOpen}>
                                    Review the consent
                                </Button>
                            </div>
                        </div>
                    )}
                </div>
            )}
            {isEulaOpen && <AiAssistantEulaModal close={toggleEulaOpen} onConsentGiven={onConsentGiven} />}
        </>
    );
}
