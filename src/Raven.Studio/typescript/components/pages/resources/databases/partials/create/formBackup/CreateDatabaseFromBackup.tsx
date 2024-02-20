import React from "react";
import { Button, CloseButton, Form, ModalBody, ModalFooter } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import Steps from "components/common/steps/Steps";
import { useAppSelector } from "components/store";
import {
    CreateDatabaseFromBackupFormData as FormData,
    createDatabaseFromBackupSchema,
} from "./createDatabaseFromBackupValidation";
import { yupResolver } from "@hookform/resolvers/yup";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useCreateDatabaseAsyncValidation } from "../shared/useCreateDatabaseAsyncValidation";
import { FormProvider, FormState, SubmitHandler, useForm, useWatch } from "react-hook-form";
import StepBasicInfo from "./steps/CreateDatabaseFromBackupStepBasicInfo";
import StepPath from "../shared/CreateDatabaseStepPath";
import StepEncryption from "../../../../../../common/FormEncryption";
import StepSource from "./steps/source/CreateDatabaseFromBackupStepSource";
import { tryHandleSubmit } from "components/utils/common";
import { DevTool } from "@hookform/devtools";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import databasesManager from "common/shell/databasesManager";
import { useCreateDatabase } from "components/pages/resources/databases/partials/create/shared/useCreateDatabase";
import { useSteps } from "components/common/steps/useSteps";
import { createDatabaseFromBackupDataUtils } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupDataUtils";
import notificationCenter from "common/notifications/notificationCenter";

interface CreateDatabaseFromBackupProps {
    closeModal: () => void;
    changeCreateModeToRegular: () => void;
}

type StepId = "basicInfo" | "backupSource" | "encryption" | "path";

// TODO move to shared
interface Step {
    id: StepId;
    label: string;
    active: boolean;
    isInvalid?: boolean;
}

// TODO google events

export default function CreateDatabaseFromBackup({
    closeModal,
    changeCreateModeToRegular,
}: CreateDatabaseFromBackupProps) {
    const { databasesService } = useServices();
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);

    const form = useForm<FormData>({
        mode: "onChange",
        defaultValues: createDatabaseFromBackupDataUtils.defaultValues,
        resolver: (data, _, options) =>
            yupResolver(createDatabaseFromBackupSchema)(
                data,
                {
                    usedDatabaseNames: usedDatabaseNames,
                    sourceType: data.source.sourceType,
                    isEncrypted: data.source.isEncrypted,
                    isSharded: data.basicInfo.isSharded,
                },
                options
            ),
    });

    const { control, setError, handleSubmit, formState, setValue, trigger } = form;
    const formValues = useWatch({
        control,
    });

    console.log("kalczur FromBackup errors", formState.errors);

    const debouncedValidationResult = useCreateDatabaseAsyncValidation(formValues.basicInfo.databaseName, setError);

    const activeSteps = getActiveStepsList(formValues, formState);

    const { currentStep, isFirstStep, isLastStep, goToStep, nextStep, prevStep } = useSteps(activeSteps.length);
    const { encryptionKeyFileName, encryptionKeyText } = useCreateDatabase(formValues);

    // TODO to function
    const stepViews: Record<StepId, JSX.Element> = {
        basicInfo: <StepBasicInfo />,
        backupSource: <StepSource />,
        encryption: (
            <StepEncryption
                control={control}
                encryptionKey={formValues.encryption.key}
                fileName={encryptionKeyFileName}
                keyText={encryptionKeyText}
                setValue={setValue}
                triggerDatabaseName={() => trigger("basicInfo.databaseName")}
                encryptionKeyFieldName="encryption.key"
                isSavedFieldName="encryption.isKeySaved"
            />
        ),
        path: <StepPath isBackupFolder manualSelectedNodes={null} />,
    };

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            if (debouncedValidationResult !== "valid") {
                return;
            }

            databasesManager.default.activateAfterCreation(formValues.basicInfo.databaseName);

            const resultDto = await databasesService.restoreDatabaseFromBackup(
                createDatabaseFromBackupDataUtils.mapToDto(formValues)
            );
            notificationCenter.instance.openDetailsForOperationById(null, resultDto.OperationId);

            closeModal();
        });
    };

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <DevTool control={control} placement="top-right" />
                <ModalBody>
                    <div className="d-flex  mb-5">
                        <Steps
                            current={currentStep}
                            steps={activeSteps.map((step) => ({ label: step.label, isInvalid: step.isInvalid }))}
                            onClick={goToStep}
                            className="flex-grow me-4"
                        ></Steps>
                        <CloseButton onClick={closeModal} />
                    </div>
                    {stepViews[activeSteps[currentStep].id]}
                </ModalBody>

                <hr />
                <ModalFooter>
                    {isFirstStep ? (
                        <Button
                            type="button"
                            onClick={changeCreateModeToRegular}
                            className="rounded-pill"
                            disabled={formState.isSubmitting}
                        >
                            <Icon icon="database" addon="star" /> Create new database
                        </Button>
                    ) : (
                        <Button type="button" onClick={prevStep} className="rounded-pill">
                            <Icon icon="arrow-thin-left" /> Back
                        </Button>
                    )}
                    <FlexGrow />
                    {isLastStep ? (
                        <ButtonWithSpinner
                            type="submit"
                            color="success"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                            icon="rocket"
                        >
                            Finish
                        </ButtonWithSpinner>
                    ) : (
                        <Button type="button" color="primary" className="rounded-pill" onClick={nextStep}>
                            Next <Icon icon="arrow-thin-right" margin="ms-1" />
                        </Button>
                    )}
                </ModalFooter>
            </Form>
        </FormProvider>
    );
}

function getActiveStepsList(formValues: FormData, formState: FormState<FormData>): Step[] {
    const allSteps: Step[] = [
        {
            id: "basicInfo",
            label: "Select backup",
            active: true,
            isInvalid: !!formState.errors.basicInfo,
        },
        { id: "backupSource", label: "Backup Source", active: true, isInvalid: !!formState.errors.source },
        {
            id: "encryption",
            label: "Encryption",
            active: formValues.source.isEncrypted,
            isInvalid: !!formState.errors.encryption,
        },
        { id: "path", label: "Paths Configuration", active: true, isInvalid: !!formState.errors.pathsConfigurations },
    ];

    return allSteps.filter((step) => step.active);
}
