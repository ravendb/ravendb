import { useFormContext, useWatch } from "react-hook-form";
import { licenseKeySchema, LicenseTypeToGenerate, SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import {
    FormCheckbox,
    FormGroup,
    FormInput,
    FormLabel,
    FormMultiRadioToggle,
    FormSelect,
    FormVerificationCodeInput,
    OptionalLabel,
} from "components/common/Form";
import { HStack } from "components/common/utilities/HStack";
import { setupWizardConstants } from "../utils/setupWizardConstants";
import Row from "react-bootstrap/Row";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import Badge from "react-bootstrap/Badge";
import messagePublisher from "common/messagePublisher";
import Modal from "components/common/Modal";
import useBoolean from "components/hooks/useBoolean";
import { useAsyncCallback, UseAsyncReturn } from "react-async-hook";
import { useEffect, useState } from "react";
import { get } from "lodash";
import { FieldPath } from "react-hook-form/dist/types/path";
import { LazyLoad } from "components/common/LazyLoad";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";

function scrollSetupWizardToTop() {
    const container = document.querySelector<HTMLElement>(".setup-wizard-main");
    container.scrollTo({ top: 0, left: 0 });
}

export function SetupWizardLicenseKeyStep() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        licenseKeyStep: { licenseTypeToGenerate },
    } = useWatch({ control });

    return (
        <div>
            {licenseTypeToGenerate == null && <NoLicenseToGenerate />}
            {licenseTypeToGenerate === "community" && <GenerateCommunity />}
            {licenseTypeToGenerate === "developer" && <GenerateDeveloper />}
        </div>
    );
}

function NoLicenseToGenerate() {
    const { reportEvent } = useEventsCollector();
    const { control, setValue } = useFormContext<SetupWizardFormData>();

    return (
        <div>
            <h2 className="mb-1">Enter license key</h2>
            <p className="mb-4 text-muted">You can either use your existing key or generate a free license.</p>

            <FormGroup>
                <FormLabel>Your key</FormLabel>
                <div className="position-relative">
                    <FormInput
                        type="textarea"
                        as="textarea"
                        control={control}
                        name="licenseKeyStep.key"
                        className="rounded-2 font-monospace"
                        placeholder={keyPlaceholder}
                        rows={16}
                        data-testid="license-key-input"
                    />
                    <LicenseKeyBadge />
                </div>
            </FormGroup>
            <div className="free-license rounded-2 p-3 mb-3 panel-bg-1 border border-secondary">
                <div className="free-license-content">
                    <h4 className="mb-0">Start with a free license today</h4>
                    <p className="text-muted">
                        Fill out a quick form to apply for a free Community or Developer license.
                    </p>
                    <Button
                        variant="outline-success"
                        className="rounded-1"
                        onClick={() => {
                            setValue("licenseKeyStep.licenseTypeToGenerate", "developer");
                            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "start-generate", "developer");
                            scrollSetupWizardToTop();
                        }}
                    >
                        Get your free license <Icon icon="arrow-thin-right" margin="m-0" />
                    </Button>
                </div>
                <svg width="219" height="129" viewBox="0 0 219 129" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <g clipPath="url(#clip0_3218_7886)">
                        <path
                            d="M147.148 31.7465L181.336 82.5625L159.907 192.113L39.3529 168.531L69.0964 16.4785L147.148 31.7465ZM55.8859 157.666L148.197 175.723L148.293 175.233L148.486 175.271L165.265 89.4968L127.229 82.0565L134.727 43.7244L80.2586 33.0696L55.8859 157.666ZM110.638 144.312L109.086 152.245L71.4382 144.881L72.9901 136.947L110.638 144.312ZM144.83 127.244L143.278 135.177L75.914 122L77.4659 114.066L144.83 127.244ZM149.364 104.066L147.812 111.999L80.4479 98.8221L81.9998 90.8885L149.364 104.066ZM143.82 70.8953L158.57 73.7805L157.896 72.779L147.379 57.146L146.705 56.1445L143.82 70.8953Z"
                            stroke="var(--border-color-light)"
                        />
                    </g>
                    <defs>
                        <clipPath id="clip0_3218_7886">
                            <rect
                                width="193.783"
                                height="193.783"
                                fill="white"
                                transform="translate(37.8359 -9.27441) rotate(11.068)"
                            />
                        </clipPath>
                    </defs>
                </svg>
            </div>
        </div>
    );
}

const keyPlaceholder = `e.g.
{
    "Id": "0cb35dc0-598b-4416-b291-088439082dc1",
    "Name": "RavenDB",
    "Keys": [
        "A1B2C3D4E5F6G7H8I9J0",
        "K1L2M3N4O5P6Q7R8S9T0",
        "U1V2W3X4Y5Z6A7B8C9D0",
        "E1F2G3H4I5J6K7L8M9N0",
        "O1P2Q3R4S5T6U7V8W9X0",
        "Y1Z2A3B4C5D6E7F8G9H0",
        "I1J2K3L4M5N6O7P8Q9R0",
        "S1T2U3V4W5X6Y7Z8A9B0",
        "C1D2E3F4G5H6I7J8K9L0"
    ]
}
`;

function LicenseKeyBadge() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        licenseKeyStep: { licenseInfo, isLoadingKey, isInvalidKey },
    } = useWatch({ control });

    if (isLoadingKey) {
        return (
            <LazyLoad active={isLoadingKey}>
                <Badge bg="secondary" pill className="position-absolute bottom-0 end-0 mb-3 me-3" style={{ zIndex: 5 }}>
                    Loading...
                </Badge>
            </LazyLoad>
        );
    }

    if (isInvalidKey) {
        return (
            <PopoverWithHoverWrapper
                wrapperClassName="position-absolute bottom-0 end-0 mb-3 me-3"
                targetStyle={{ zIndex: 5 }}
                message="The provided license key format is invalid. Please check the key and try again."
            >
                <Badge bg="danger" pill>
                    Invalid
                    <Icon icon="warning" margin="ms-2" />
                </Badge>
            </PopoverWithHoverWrapper>
        );
    }

    if (licenseInfo == null || licenseInfo.licenseType == null) {
        return null;
    }

    const bg = (() => {
        switch (licenseInfo.licenseType) {
            case "Community":
            case "Essential":
                return "info";
            case "Developer":
                return "developer";
            case "Professional":
                return "professional";
            case "Enterprise":
                return "primary";
            default:
                return "secondary";
        }
    })();

    return (
        <Badge bg={bg} pill className="position-absolute bottom-0 end-0 mb-3 me-3" style={{ zIndex: 5 }}>
            {licenseInfo.licenseType}
        </Badge>
    );
}

function GenerateCommunity() {
    return (
        <div>
            <HStack className="justify-content-between flex-wrap align-items-start mb-1">
                <h2 className="m-0">
                    Generate new <span className="text-community">Community</span> license
                </h2>
                <LicenseTypeRadio />
            </HStack>
            <p className="text-muted">
                Good for teams who are just starting out and simply want the essentials.
                <br />
                <br />
                <Icon icon="check" color="community" /> Eligible for commercial use
                <br />
                <Icon icon="check" color="community" /> Access to all basic features
                <br />
                <Icon icon="check" color="community" /> Max of 3 nodes in cluster, 3 CPU cores, and 6 GB RAM
            </p>
            <SeeAllPlansButton />
            <GenerateLicenseFields />
        </div>
    );
}

function GenerateDeveloper() {
    return (
        <div>
            <HStack className="justify-content-between flex-wrap align-items-start mb-1">
                <h2 className="m-0">
                    Generate new <span className="text-developer">Developer</span> license
                </h2>
                <LicenseTypeRadio />
            </HStack>
            <p className="text-muted">
                Recommended for teams who want to test & develop RavenDB in it&apos;s full potential.
                <br />
                <br />
                <Icon icon="cancel" color="developer" /> Not applicable for commercial use
                <br />
                <Icon icon="check" color="developer" /> Enterprise-level set of features
                <br />
                <Icon icon="check" color="developer" /> Max of 5 nodes in cluster, 9 CPU cores, and 36 GB RAM
            </p>
            <SeeAllPlansButton />
            <GenerateLicenseFields />
        </div>
    );
}

function GenerateLicenseFields() {
    const { control } = useFormContext<SetupWizardFormData>();

    return (
        <Row className="w-100 mt-4">
            <FormGroup className="col-md-6">
                <FormLabel>First name</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="licenseKeyStep.firstName"
                    placeholder="Your first name"
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Last name</FormLabel>
                <FormInput type="text" control={control} name="licenseKeyStep.lastName" placeholder="Your last name" />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Email</FormLabel>
                <FormInput type="email" control={control} name="licenseKeyStep.email" placeholder="Your email" />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>
                    Phone <OptionalLabel />
                </FormLabel>
                <FormInput type="text" control={control} name="licenseKeyStep.phone" placeholder="Your phone number" />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Country</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.allCountries.map((x) => ({ value: x, label: x }))}
                    name="licenseKeyStep.country"
                    placeholder="Your country"
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Job title</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.allJobTitles.map((x) => ({ value: x, label: x }))}
                    name="licenseKeyStep.jobTitle"
                    placeholder="Choose your job title"
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Industry</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.allIndustries.map((x) => ({ value: x, label: x }))}
                    name="licenseKeyStep.industry"
                    placeholder="Choose your industry"
                />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>Company</FormLabel>
                <FormInput type="text" control={control} name="licenseKeyStep.company" placeholder="Your company" />
            </FormGroup>
            <FormGroup className="col-md-6">
                <FormLabel>How do you plan to use RavenDB?</FormLabel>
                <FormSelect
                    control={control}
                    options={setupWizardConstants.howYouPlanToUseRavenDBOptions}
                    name="licenseKeyStep.howYouPlanToUseRavenDB"
                    placeholder="Choose the most suitable option"
                />
            </FormGroup>
            <FormGroup marginClass="mb-0 mt-2">
                <FormCheckbox control={control} name="licenseKeyStep.isAcceptTerms">
                    I accept the terms and conditions
                </FormCheckbox>
            </FormGroup>
            <FormGroup>
                <FormCheckbox control={control} name="licenseKeyStep.isAcceptEmails">
                    I would like to receive learning materials and occasional marketing emails <OptionalLabel />
                </FormCheckbox>
            </FormGroup>
        </Row>
    );
}

function SeeAllPlansButton() {
    return (
        <Button href="https://ravendb.net/buy" target="_blank" variant="link" className="p-0">
            See all plans in detail <Icon icon="newtab" />
        </Button>
    );
}

function LicenseTypeRadio() {
    const { control } = useFormContext<SetupWizardFormData>();

    return (
        <div>
            <FormMultiRadioToggle
                control={control}
                name="licenseKeyStep.licenseTypeToGenerate"
                inputItems={[
                    {
                        label: "Community",
                        value: "community",
                        badgeColor: "secondary",
                        icon: <Icon icon="community" />,
                    },
                    {
                        label: "Developer",
                        value: "developer",
                        badgeColor: "secondary",
                        icon: <Icon icon="console" />,
                    },
                ]}
                className="d-flex justify-content-center"
            />
        </div>
    );
}

function SkipLicenseVerificationConfirmModal(props: { close: () => void }) {
    const { reportEvent } = useEventsCollector();
    const { close } = props;
    const { setValue } = useFormContext<SetupWizardFormData>();

    const handleConfirm = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "skip-verification", "confirmed");
        setValue("currentStep", "Security");
    };

    const handleLicenseTypeChange = (licenseType: LicenseTypeToGenerate) => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "choose-generate", licenseType);
        setValue("licenseKeyStep.licenseTypeToGenerate", licenseType);
        close();
    };

    return (
        <Modal show onHide={close} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header closeButton onCloseClick={close} className="pb-0">
                <h3 className="mb-0">
                    <Icon icon="license" color="warning" />
                    You&#39;re about to skip license verification
                </h3>
            </Modal.Header>

            <Modal.Body className="pt-0">
                <p className="my-3">While you&apos;ll be able to use RavenDB, there will be some limitations:</p>
                <Icon icon="check" color="success" />{" "}
                <a href="https://www.gnu.org/licenses/agpl-3.0.en.html" target="_blank">
                    AGPLv3 restrictions
                </a>{" "}
                applied
                <br />
                <Icon icon="check" color="success" /> Limited set of features
                <br />
                <Icon icon="check" color="success" /> Max of 1 node in cluster, 3 CPU cores, and 6 GB RAM memory usage
                <p className="mt-3 mb-0">
                    Either confirm your choice and skip the verification, or generate a new{" "}
                    <Button
                        variant="link"
                        className="text-info p-0 text-decoration-underline"
                        onClick={() => handleLicenseTypeChange("community")}
                        title="Generate Community license"
                    >
                        Community
                    </Button>{" "}
                    or{" "}
                    <Button
                        variant="link"
                        className="text-developer p-0 text-decoration-underline"
                        onClick={() => handleLicenseTypeChange("developer")}
                        title="Generate Developer license"
                    >
                        Developer
                    </Button>{" "}
                    license.
                </p>
            </Modal.Body>
            <Modal.Footer>
                <Button
                    variant="link"
                    onClick={() => {
                        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "skip-verification", "cancel");
                        close();
                    }}
                    className="link-muted"
                >
                    Cancel
                </Button>
                <Button variant="warning" onClick={handleConfirm} className="rounded-pill d-flex align-items-center">
                    Skip verification &nbsp;
                    <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

interface SetupWizardLicenseKeyVerifyCodeModalProps {
    close: () => void;
    sendLicenseVerificationCode: UseAsyncReturn<void, []>;
}

export function SetupWizardLicenseKeyVerifyCodeModal({
    close,
    sendLicenseVerificationCode,
}: SetupWizardLicenseKeyVerifyCodeModalProps) {
    const { reportEvent } = useEventsCollector();
    const licenseKeyStepData: SetupWizardFormData["licenseKeyStep"] = useWatch<SetupWizardFormData>({
        name: "licenseKeyStep",
    });

    const { licenseService } = useServices();
    const {
        control,
        setValue,
        setError,
        clearErrors,
        setFocus,
        formState: { errors },
    } = useFormContext<SetupWizardFormData>();
    const { countdown, isCountdownActive, startCountdown } = useResendCountdown();

    useEffect(() => {
        // clear errors on remounting component
        setFocus("licenseKeyStep.verificationCode");
        if (get(errors, "licenseKeyStep.verificationCode")) {
            clearErrors("licenseKeyStep.verificationCode");
        }
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "open-verify-modal");
    }, []);

    const onSubmitVerifiedCode = useAsyncCallback(
        async (inputCode: string) => {
            const license = await licenseService.verifyLicense({
                Email: licenseKeyStepData.email,
                VerificationCode: inputCode,
            });

            if (license.LicenseDownloadStatus !== "Success") {
                throw new Error(license.LicenseDownloadStatus);
            }
            return license;
        },
        {
            onSuccess: async (license) => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "verify-code", "success");
                setValue("licenseKeyStep.key", JSON.stringify(license.License, null, 2), {
                    shouldDirty: true,
                });
                close();
                setValue("licenseKeyStep.licenseTypeToGenerate", null);
            },
            onError: async (error) => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "verify-code", error.message);
                setError("licenseKeyStep.verificationCode", {
                    message: convertVerificationCodeErrorMessage(error.message as FreeLicenseDownloadStatus),
                });
            },
        }
    );

    const handleResendClick = async () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "resend-code");
        await sendLicenseVerificationCode.execute().then(() => {
            startCountdown();
        });
    };

    return (
        <Modal
            show
            onHide={() => {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                close();
            }}
            contentClassName="modal-border bulge-primary"
            size="lg"
        >
            <Modal.Header
                closeButton
                onCloseClick={() => {
                    reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                    close();
                }}
                className="pb-0"
            >
                <h3 className="d-flex justify-content-center align-items-center w-100">
                    <Icon icon="paperplane" color="primary" />
                    Enter verification code
                </h3>
            </Modal.Header>
            <Modal.Body className="d-flex align-items-center flex-column justify-items-center">
                <p className="text-center mb-1">Before generating license we need to confirm provided email.</p>
                <p>
                    Please check you email inbox. We have sent verification code to <b>{licenseKeyStepData?.email}</b>
                </p>
                <form>
                    <FormVerificationCodeInput
                        onLastDigitInsertSubmit={onSubmitVerifiedCode.execute}
                        name="licenseKeyStep.verificationCode"
                        control={control}
                    />
                </form>
                <p className="text-center mt-3">
                    Did not get a code?{" "}
                    <Button
                        variant="link"
                        className="m-0 p-0"
                        onClick={handleResendClick}
                        disabled={sendLicenseVerificationCode.loading || isCountdownActive}
                    >
                        {isCountdownActive
                            ? `Resend in ${countdown}s`
                            : sendLicenseVerificationCode.loading
                              ? "Sending..."
                              : "Click to resend"}
                    </Button>{" "}
                    or update your email address.
                </p>
            </Modal.Body>
            <Modal.Footer>
                <Button
                    variant="link"
                    onClick={() => {
                        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "close-verify-modal");
                        close();
                    }}
                    className="link-muted"
                >
                    Cancel
                </Button>
                <ButtonWithSpinner
                    isSpinning={onSubmitVerifiedCode.loading}
                    variant="primary"
                    disabled={licenseKeyStepData.verificationCode?.length !== 6}
                    className="rounded-pill"
                    onClick={() => onSubmitVerifiedCode.execute(licenseKeyStepData.verificationCode)}
                >
                    Verify email
                    <Icon icon="arrow-right" margin="ms-1" />
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}

function useResendCountdown(initialDelay: number = 30) {
    const [countdown, setCountdown] = useState(0);
    const [isActive, setIsActive] = useState(false);

    useEffect(() => {
        let interval: NodeJS.Timeout;
        if (isActive && countdown > 0) {
            interval = setInterval(() => {
                setCountdown((prev) => prev - 1);
            }, 1000);
        } else if (countdown === 0) {
            setIsActive(false);
        }
        return () => clearInterval(interval);
    }, [isActive, countdown]);

    const startCountdown = () => {
        setCountdown(initialDelay);
        setIsActive(true);
    };

    return { countdown, isCountdownActive: isActive, startCountdown };
}

function convertVerificationCodeErrorMessage(error: FreeLicenseDownloadStatus) {
    switch (error) {
        case "CodeAlreadyUsed":
            return "Code already used";
        case "CodeExpired":
            return "Code expired";
        case "InvalidCredentials":
            return "Invalid credentials";
        default:
            return error;
    }
}

export function SetupWizardLicenseKeyStepFooter() {
    const { reportEvent } = useEventsCollector();
    const { control, setValue, trigger } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();

    const { value: isLicenseSkipModalOpen, toggle: toggleIsLicenseSkipModalOpen } = useBoolean(false);
    const { value: isLicenseActivationCodeModalOpen, toggle: toggleIsLicenseActivationCodeModalOpen } =
        useBoolean(false);

    const { licenseKeyStep } = useWatch({ control });

    const { key, licenseTypeToGenerate, isInvalidKey } = licenseKeyStep;

    const toDto = (licenseStepData: SetupWizardFormData["licenseKeyStep"]): SendFreeLicenseVerificationRequest => {
        if (!licenseStepData) {
            return;
        }

        return {
            Type: licenseStepData.licenseTypeToGenerate === "developer" ? "Developer" : "Community",
            LicenseType: licenseStepData.licenseTypeToGenerate === "developer" ? "Developer" : "Community",
            FirstName: licenseStepData.firstName,
            LastName: licenseStepData.lastName,
            Email: licenseStepData.email,
            Company: licenseStepData.company,
            Country: licenseStepData.country,
            JobTitle: licenseStepData.jobTitle,
            Industry: licenseStepData.industry,
            HowDoYouPlanToUseRavenDb: licenseStepData.howYouPlanToUseRavenDB,
            AcceptTheTermsAndConditions: !!licenseStepData.isAcceptTerms,
            MarketingConsent: !!licenseStepData.isAcceptEmails,
        };
    };
    const asyncRegistrationInfo = useAsyncDebounce(
        async () => {
            setValue("licenseKeyStep.isLoadingKey", true);
            setValue("licenseKeyStep.isInvalidKey", false);
            setValue("licenseKeyStep.licenseInfo", null);

            if (key == null) {
                setValue("licenseKeyStep.isLoadingKey", false);
                return;
            }

            if (!key) {
                setValue("licenseKeyStep.isLoadingKey", false);
                return;
            }

            try {
                const parsedKey = JSON.parse(key);
                await licenseKeySchema.validate(parsedKey);

                const info = await setupWizardService.registrationInfo(parsedKey);

                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "key-parse", "valid");
                setValue(
                    "licenseKeyStep.licenseInfo",
                    {
                        licenseType: info.LicenseType,
                        userDomainsWithIps: {
                            email: info.UserDomainsWithIps.Emails,
                            rootDomains: info.UserDomainsWithIps.RootDomains,
                            domains: info.UserDomainsWithIps.Domains,
                        },
                        maxClusterSize: info.MaxClusterSize,
                    },
                    {
                        shouldDirty: true,
                    }
                );
                setValue("licenseKeyStep.isLoadingKey", false);
            } catch (err) {
                reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "key-parse", "invalid");
                setValue("licenseKeyStep.isInvalidKey", true);
                setValue("licenseKeyStep.isLoadingKey", false);
                setValue("licenseKeyStep.licenseInfo", null);
            }
        },
        [key],
        300
    );

    const { licenseService } = useServices();

    const asyncSendLicenseVerificationCode = useAsyncCallback(() =>
        licenseService.sendVerificationCode(toDto(licenseKeyStep))
    );

    const handleAlreadyHaveLicense = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "already-have-license");
        setValue("licenseKeyStep.licenseTypeToGenerate", null);
        scrollSetupWizardToTop();
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "back");
        setValue("currentStep", "Setup method");
        scrollSetupWizardToTop();
        setValue("licenseKeyStep", setupWizardFormDefaultValues["licenseKeyStep"]);
    };

    const handleGenerateLicense = async () => {
        const fields: FieldPath<SetupWizardFormData>[] = [
            "licenseKeyStep.firstName",
            "licenseKeyStep.lastName",
            "licenseKeyStep.email",
            "licenseKeyStep.company",
            "licenseKeyStep.country",
            "licenseKeyStep.jobTitle",
            "licenseKeyStep.industry",
            "licenseKeyStep.howYouPlanToUseRavenDB",
            "licenseKeyStep.isAcceptTerms",
        ];
        const isValid = await trigger(fields);

        if (isValid) {
            reportEvent(
                setupWizardGA4Prefixes.licenseKeyStep,
                "send-verification-code",
                licenseTypeToGenerate ?? "unknown"
            );
            await asyncSendLicenseVerificationCode.execute();
            messagePublisher.reportSuccess("Verification code sent to your email");
            toggleIsLicenseActivationCodeModalOpen();
        } else {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "send-verification-code", "validation-failed");
        }
    };

    const handleContinue = async () => {
        if (key) {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "continue", "with-key");
            setValue("currentStep", "Security");
        } else {
            reportEvent(setupWizardGA4Prefixes.licenseKeyStep, "open-skip-modal");
            toggleIsLicenseSkipModalOpen();
        }
    };

    return (
        <div className="d-flex justify-content-between">
            {licenseTypeToGenerate != null ? (
                <Button variant="outline-secondary" className="rounded-pill" onClick={handleAlreadyHaveLicense}>
                    <Icon icon="license" /> I already have a license
                </Button>
            ) : (
                <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                    <Icon icon="arrow-left" /> Back
                </Button>
            )}

            {licenseTypeToGenerate != null ? (
                <Button variant="primary" className="rounded-pill" onClick={handleGenerateLicense}>
                    Generate license <Icon icon="arrow-right" margin="m-0" />
                </Button>
            ) : (
                <ButtonWithSpinner
                    disabled={isInvalidKey}
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                    isSpinning={asyncRegistrationInfo.loading}
                >
                    Continue <Icon icon="arrow-right" margin="m-s1" />
                </ButtonWithSpinner>
            )}
            {isLicenseSkipModalOpen && <SkipLicenseVerificationConfirmModal close={toggleIsLicenseSkipModalOpen} />}
            {isLicenseActivationCodeModalOpen && (
                <SetupWizardLicenseKeyVerifyCodeModal
                    sendLicenseVerificationCode={asyncSendLicenseVerificationCode}
                    close={toggleIsLicenseActivationCodeModalOpen}
                />
            )}
        </div>
    );
}
