import { Controller, useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import messagePublisher from "common/messagePublisher";
import FileDropzone from "components/common/FileDropzone";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import useBoolean from "components/hooks/useBoolean";
import Form from "react-bootstrap/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import React, { useMemo } from "react";
import { useAsyncDebounce } from "hooks/useAsyncDebounce";
import { useAppDispatch, useAppSelector } from "components/store";
import { setupWizardActions, setupWizardSelectors } from "components/setupWizard/store/setupWizardSlice";
import { base64ToFile, fileToBase64 } from "components/setupWizard/utils/setupWizardUtils";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import { LazyLoad } from "components/common/LazyLoad";

export function SetupWizardSelfSignedCertificateStep() {
    const { control, setValue, clearErrors, setError } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();
    const dispatch = useAppDispatch();

    const { value: isFileProtected, setValue: setIsFileProtected } = useBoolean(false);
    const { value: isPasswordReadOnly, setValue: setIsPasswordReadOnly } = useBoolean(false);
    const {
        selfSignedCertificateStep: { certificate, password, cns, certificateFileName },
    } = useWatch({ control });

    const certificateHasPassword = useAppSelector(setupWizardSelectors.selfSignedCertificateStepHasPassword);

    const cnsDebounce = useAsyncDebounce(
        async () => {
            setValue("selfSignedCertificateStep.cns", []);

            if (!certificate) {
                return;
            }

            return setupWizardService.listHostsForCertificate(certificate, password);
        },
        [password, certificate],
        300,
        {
            onSuccess: (cns) => {
                const uniqueCns = Array.from(new Set(cns));
                setValue("selfSignedCertificateStep.cns", uniqueCns);

                const isWildcardCertificate = uniqueCns.some((cn) => cn.startsWith("*"));
                setValue("selfSignedCertificateStep.isWildcardCertificate", isWildcardCertificate);

                clearErrors("selfSignedCertificateStep.password");
                dispatch(setupWizardActions.selfSignedCertificateStepIsPasswordValidSet(true));

                if (!password) {
                    setIsFileProtected(false);
                    dispatch(setupWizardActions.selfSignedCertificateStepHasPasswordSet(false));
                }

                if (password) {
                    setIsPasswordReadOnly(true);
                }
            },
            onError: (error) => {
                if ((error as unknown as JQueryXHR).status === 400) {
                    setIsPasswordReadOnly(false);
                    setIsFileProtected(true);
                    dispatch(setupWizardActions.selfSignedCertificateStepHasPasswordSet(true));
                    dispatch(setupWizardActions.selfSignedCertificateStepIsPasswordValidSet(false));

                    if (password.length > 0) {
                        setError("selfSignedCertificateStep.password", { message: "Invalid password" });
                    }
                }
            },
        }
    );

    const clearFile = () => {
        setValue("selfSignedCertificateStep.certificate", "");
        setValue("selfSignedCertificateStep.certificateFileName", "");
        setValue("selfSignedCertificateStep.cns", []);
    };

    const getInitialFiles = useMemo((): File[] => {
        if (certificate && certificateFileName) {
            try {
                return [base64ToFile(certificate, certificateFileName)];
            } catch (error) {
                messagePublisher.reportError("Failed to load file", error);
                return [];
            }
        }
        return [];
    }, [certificate, certificateFileName]);

    return (
        <div>
            <h2 className="mb-1">Use self-signed certificate</h2>
            <p className="mb-4 text-muted">
                For the highest level of security and control, you can use your own certificate once it is obtained.
            </p>
            <FormGroup>
                <Controller
                    render={({ field }) => (
                        <FileDropzone
                            validExtensions={["pfx"]}
                            maxFiles={1}
                            initialFiles={getInitialFiles}
                            {...field}
                            onChange={async (files: File[]) => {
                                const file = files[0];
                                if (!file.name.trim()) {
                                    clearFile();
                                    messagePublisher.reportError("Failed to load file");
                                    return;
                                }

                                try {
                                    const fileInString = await fileToBase64(file);
                                    const cleanFileInBase64 = fileInString.substring(fileInString.indexOf(",") + 1);

                                    setValue("selfSignedCertificateStep.certificate", cleanFileInBase64);
                                    setValue("selfSignedCertificateStep.certificateFileName", file.name);

                                    field.onChange(cleanFileInBase64);
                                } catch (e) {
                                    clearFile();
                                    messagePublisher.reportError("Failed to load file", e.message);
                                }
                            }}
                        />
                    )}
                    name="selfSignedCertificateStep.certificate"
                    control={control}
                />
            </FormGroup>
            {(certificateHasPassword || isFileProtected || (isFileProtected && isPasswordReadOnly)) && (
                <FormGroup>
                    <FormLabel className="hstack">
                        Passphrase
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Enter the passphrase used to encrypt your private key. 
                                        This passphrase is required to unlock and use your certificate."
                                    ravenLinkHash="VBORN2"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        disabled={isPasswordReadOnly}
                        type="password"
                        control={control}
                        passwordPreview
                        name="selfSignedCertificateStep.password"
                        placeholder="Enter your passphrase"
                    />
                </FormGroup>
            )}
            {cns.length > 0 && (
                <FormGroup>
                    <FormLabel className="hstack">
                        CN Names
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="The common name (CN) of the certificate. This is the name that will be displayed in the browser when you access the server."
                                    ravenLinkHash="VBORN2"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <LazyLoad active={cnsDebounce.loading}>
                        <div className="vstack gap-1">
                            {cns.map((cn) => (
                                <div className="panel-bg-2">
                                    <Form.Control key={cn} type="text" value={cn} disabled readOnly />
                                </div>
                            ))}
                        </div>
                    </LazyLoad>
                </FormGroup>
            )}
        </div>
    );
}

export function SetupWizardSelfSignedCertificateStepFooter() {
    const { setValue, control } = useFormContext<SetupWizardFormData>();
    const isPasswordValid = useAppSelector(setupWizardSelectors.selfSignedCertificateStepIsPasswordValid);

    const {
        selfSignedCertificateStep: { certificate },
    } = useWatch({ control });

    const handleContinue = () => {
        setValue("currentStep", "Node addresses");
    };

    const handleBack = () => {
        setValue("currentStep", "Security");
        setValue("selfSignedCertificateStep", setupWizardFormDefaultValues["selfSignedCertificateStep"]);
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <Button
                variant="primary"
                className="rounded-pill"
                disabled={!certificate || !isPasswordValid}
                onClick={handleContinue}
            >
                Continue <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}
