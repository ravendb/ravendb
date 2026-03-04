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
            <p className="mb-4 text-muted">Choose how to secure your RavenDB server.</p>
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
                        description="Automatically issue and renew a trusted HTTPS certificate using your domain"
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
                                    Default choice for most users. Recommended if you don&#39;t need a custom
                                    certificate and prefer RavenDB to manage encryption without any manual setup.
                                </li>
                                <li className="mt-1">
                                    RavenDB will automatically issue and renew a trusted HTTPS certificate for your
                                    domain (chosen in the next step).
                                </li>
                                <li className="mt-1">
                                    Ideal for standard deployments where a public domain is available and you want a
                                    simple, secure default option.
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
                                    Use this when you need to provide a custom SSL/TLS certificate, often to comply with
                                    corporate security policies or integrate with an internal certificate authority.
                                </li>
                                <li className="mt-1">
                                    Recommended for <b>production environments</b> where certificates are managed
                                    manually or by external infrastructure, and full control over certificate management
                                    and trust configuration is required.
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
                    description="Suitable only for quick local development with no security requirements"
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
                                Use only in <b>trusted, isolated environments</b> such as local development, internal
                                testing, or secure private networks where encryption is intentionally not required
                                (e.g., for performance or testing purposes).
                            </li>
                            <li className="mt-1">
                                <b>Not recommended for production</b>, as all traffic will be transmitted without
                                encryption or authentication, making it vulnerable to eavesdropping or man-in-the-middle
                                attacks.
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

    const isContinueDisabled =
        (!isLetsEncryptAgreementAccepted && securityOption === "letsEncrypt") || securityOption === null;

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
                    disabled={isContinueDisabled}
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
