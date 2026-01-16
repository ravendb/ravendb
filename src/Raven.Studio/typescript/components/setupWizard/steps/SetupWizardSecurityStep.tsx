import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import SetupWizardClickableCard from "../partials/SetupWizardClickableCard";
import Button from "react-bootstrap/Button";
import { FormCheckbox } from "components/common/Form";
import assertUnreachable from "components/utils/assertUnreachable";
import Badge from "react-bootstrap/Badge";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import Spinner from "react-bootstrap/Spinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEffect } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";

export function SetupWizardSecurityStep() {
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const {
        securityStep: { securityOption },
        licenseKeyStep: { key, licenseInfo },
    } = useWatch({ control });

    useEffect(() => {
        if (isSecureDisabled) {
            setValue("securityStep.securityOption", "none");
        }
        if (isSecureRecommended) {
            setValue("securityStep.securityOption", "letsEncrypt");
        }
    }, []);

    const isSecureDisabled = !key;
    const isSecureRecommended = !!key && licenseInfo?.licenseType !== "Developer";

    return (
        <div>
            <h2 className="mb-1">Security</h2>
            <p className="mb-4 text-muted">Select the security option that best addresses your needs</p>
            <div className="mt-4">
                <h5 className="mb-1 d-flex align-items-center">
                    <Icon icon="lock" color="success" />
                    Secure
                    {isSecureRecommended && (
                        <PopoverWithHoverWrapper
                            message={
                                <div>
                                    You&#39;re using a Production License, so we highly recommend choosing a{" "}
                                    <b>secure setup with a certificate</b>. This ensures optimal data protection,
                                    encrypted communication, and compliance with best security practices for your
                                    production environment.
                                </div>
                            }
                        >
                            <Badge pill bg="faded-success" className="ms-1">
                                Recommended
                            </Badge>
                        </PopoverWithHoverWrapper>
                    )}
                </h5>
                <ConditionalPopover
                    className="w-100"
                    conditions={{
                        isActive: isSecureDisabled,
                        message: (
                            <div>
                                Secure setup methods are not available without a license. If you&#39;d like to use one
                                of the secure options, you can go back to <b>License Key</b> step and insert an existing
                                license or generate a free <b className="text-info">Community</b> or{" "}
                                <b className="text-developer">Developer</b> license.
                            </div>
                        ),
                    }}
                >
                    <SetupWizardClickableCard
                        className="w-100"
                        icon="lets-encrypt"
                        title="Generate Let's Encrypt certificate"
                        description="Secure and hassle-free communication with automatic certificate management"
                        isSelected={securityOption === "letsEncrypt"}
                        onClick={() => {
                            setValue("securityStep.securityOption", "letsEncrypt", {
                                shouldDirty: true,
                            });
                            reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "letsEncrypt");
                        }}
                        isDisabled={isSecureDisabled}
                        popoverMessage={
                            <ul className="mb-0 ps-3">
                                <li>
                                    Default setting for most users. RavenDB will automatically generate and manage
                                    SSL/TLS certificates for encrypting communications between nodes and clients.
                                </li>
                                <li>
                                    Ideal when you don&#39;t have a specific custom certificate or prefer RavenDB to
                                    handle encryption automatically.
                                </li>
                            </ul>
                        }
                    />
                </ConditionalPopover>
                <ConditionalPopover
                    className="w-100"
                    conditions={{
                        isActive: isSecureDisabled,
                        message: (
                            <div>
                                Secure setup methods are not available without a license. If you&#39;d like to use one
                                of the secure options, you can go back to <b>License Key</b> step and insert an existing
                                license or generate a free <b className="text-info">Community</b> or{" "}
                                <b className="text-developer">Developer</b> license.
                            </div>
                        ),
                    }}
                >
                    <SetupWizardClickableCard
                        className="mt-2 w-100"
                        icon="certificate"
                        title="Provide your own certificate"
                        description="Ideal for secure corporate setups with manual certificate management"
                        isSelected={securityOption === "ownCertificate"}
                        onClick={() => {
                            setValue("securityStep.securityOption", "ownCertificate", {
                                shouldDirty: true,
                            });
                            reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "ownCertificate");
                        }}
                        isDisabled={isSecureDisabled}
                        popoverMessage={
                            <ul className="mb-0 ps-3">
                                <li>
                                    You need to use a custom SSL/TLS certificate, often for integration with a specific
                                    internal certificate authority or to comply with corporate security policies.
                                </li>
                                <li>
                                    Useful if you need to integrate RavenDB with an existing private infrastructure that
                                    requires specific certificates.
                                </li>
                                <li>
                                    Ideal for <b>production environments</b> where you want more control over
                                    certificate management and trust settings.
                                </li>
                            </ul>
                        }
                    />
                </ConditionalPopover>
            </div>
            <div className="my-4">
                <h5 className="mb-1">
                    <Icon icon="lock" color="warning" />
                    Unsecure
                </h5>
                <SetupWizardClickableCard
                    icon="empty-set"
                    title="Don't use certificate"
                    description="Best for quick local development with no security requirements"
                    isSelected={securityOption === "none"}
                    onClick={() => {
                        setValue("securityStep.securityOption", "none", {
                            shouldDirty: true,
                        });
                        reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "none");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>
                                Only in <b>trusted, isolated environments</b> (e.g., internal testing, local
                                development, or a network that is isolated from the public internet).
                            </li>
                            <li>
                                <b>Not recommended for production</b> environments as data will be transmitted
                                unencrypted, leaving it vulnerable to eavesdropping or man-in-the-middle attacks.
                            </li>
                            <li>
                                If you are aware that all nodes are within a secure, private network and you don&#39;t
                                need encryption for performance or resource constraints.
                            </li>
                        </ul>
                    }
                />
            </div>
        </div>
    );
}

export function SetupWizardSecurityStepFooter() {
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();

    const {
        securityStep: { securityOption, isLetsEncryptAgreementAccepted },
        licenseKeyStep: { licenseInfo },
    } = useWatch({ control });

    const asyncGetLetsEncryptAgreement = useAsync(
        () => setupWizardService.getLetsEncryptAgreement(licenseInfo.userDomainsWithIps.email[0] ?? ""),
        []
    );

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.securityStep, "back");
        setValue("currentStep", "License key");
        setValue("securityStep", setupWizardFormDefaultValues["securityStep"]);
    };

    const handleContinue = () => {
        reportEvent(setupWizardGA4Prefixes.securityStep, "continue", securityOption);
        switch (securityOption) {
            case "letsEncrypt":
                setValue("currentStep", "Domain");
                break;
            case "ownCertificate":
                setValue("currentStep", "Self-signed certificate");
                break;
            case "none":
                setValue("currentStep", "Node addresses");
                break;
            default:
                assertUnreachable(securityOption);
        }
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <div className="hstack gap-2 align-items-center">
                {securityOption === "letsEncrypt" && (
                    <FormCheckbox
                        disabled={asyncGetLetsEncryptAgreement.loading}
                        className="mb-0"
                        control={control}
                        name="securityStep.isLetsEncryptAgreementAccepted"
                    >
                        I accept {asyncGetLetsEncryptAgreement.loading && <Spinner />}
                        <a target="_blank" href={asyncGetLetsEncryptAgreement.result as string}>
                            Let&apos;s Encrypt Subscriber Agreement
                        </a>
                    </FormCheckbox>
                )}
                <Button
                    disabled={!isLetsEncryptAgreementAccepted && securityOption === "letsEncrypt"}
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                >
                    Continue <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </div>
        </div>
    );
}
