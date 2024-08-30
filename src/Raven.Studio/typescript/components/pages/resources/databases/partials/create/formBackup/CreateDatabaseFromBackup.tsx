import React, { useCallback } from "react";
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
import {
    Control,
    FieldErrors,
    FormProvider,
    SubmitHandler,
    UseFormSetValue,
    UseFormTrigger,
    useForm,
    useWatch,
} from "react-hook-form";
import StepBasicInfo from "./steps/CreateDatabaseFromBackupStepBasicInfo";
import StepPath from "../shared/CreateDatabaseStepDataDirectory";
import StepEncryption from "../../../../../../common/FormEncryption";
import StepSource from "./steps/source/CreateDatabaseFromBackupStepSource";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useSteps } from "components/common/steps/useSteps";
import { createDatabaseFromBackupDataUtils } from "./createDatabaseFromBackupDataUtils";
import notificationCenter from "common/notifications/notificationCenter";
import { CreateDatabaseStep, createDatabaseUtils } from "../shared/createDatabaseUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useCreateDatabaseShortcuts } from "../shared/useCreateDatabaseShortcuts";

interface CreateDatabaseFromBackupProps {
    closeModal: () => void;
    changeCreateModeToRegular: () => void;
}

export default function CreateDatabaseFromBackup({
    closeModal,
    changeCreateModeToRegular,
}: CreateDatabaseFromBackupProps) {
    const { databasesService } = useServices();
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);

    const form = useForm<FormData>({
        mode: "onChange",
        defaultValues: createDatabaseFromBackupDataUtils.defaultValues,
        resolver: (data, _, options) =>
            yupResolver(createDatabaseFromBackupSchema)(
                data,
                {
                    usedDatabaseNames: usedDatabaseNames,
                    sourceType: data.sourceStep.sourceType,
                    isEncrypted: data.sourceStep.isEncrypted,
                    isSharded: data.basicInfoStep.isSharded,
                    localNodeTag,
                },
                options
            ),
    });

    const { control, setError, handleSubmit, formState, setValue, trigger } = form;
    const formValues = useWatch({
        control,
    });

    const asyncDatabaseNameValidation = useCreateDatabaseAsyncValidation(
        formValues.basicInfoStep.databaseName,
        setError
    );

    const activeSteps = getActiveStepsList(formValues, formState.errors, asyncDatabaseNameValidation.loading);
    const stepViews = getStepViews(control, formValues, setValue, trigger);
    const { currentStep, isFirstStep, isLastStep, goToStepWithValidation, nextStepWithValidation, prevStep } = useSteps(
        activeSteps.length
    );

    const validateToTargetStep = useCallback(
        (targetStep: number) => {
            return createDatabaseUtils.getStepInRangeValidation({
                currentStep,
                targetStep,
                activeStepIds: activeSteps.map((x) => x.id),
                trigger,
                asyncDatabaseNameValidation,
                databaseName: formValues.basicInfoStep.databaseName,
            });
        },
        [activeSteps, asyncDatabaseNameValidation, currentStep, formValues.basicInfoStep.databaseName, trigger]
    );

    const { reportEvent } = useEventsCollector();

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            reportEvent("database", "restore");

            // It causes errors when fetching stats. Commented for now
            // databasesManager.default.activateAfterCreation(formValues.basicInfoStep.databaseName);

            const resultDto = await databasesService.restoreDatabaseFromBackup(
                createDatabaseFromBackupDataUtils.mapToDto(formValues)
            );

            try {
                notificationCenter.instance.openDetailsForOperationById(null, resultDto.OperationId);
            } finally {
                closeModal();
            }
        });
    };

    const handleGoNext = useCallback(async () => {
        await nextStepWithValidation(validateToTargetStep(currentStep));
    }, [currentStep, nextStepWithValidation, validateToTargetStep]);

    useCreateDatabaseShortcuts({
        submit: handleSubmit(onFinish),
        handleGoNext,
        isLastStep,
        canQuickCreate: false,
    });

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <ModalBody>
                    <div className="d-flex  mb-5">
                        <Steps
                            current={currentStep}
                            steps={activeSteps.map(createDatabaseUtils.mapToStepItem)}
                            onClick={(step) => goToStepWithValidation(step, validateToTargetStep(step - 1))}
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
                        <Button type="button" color="primary" className="rounded-pill" onClick={handleGoNext}>
                            Next <Icon icon="arrow-thin-right" margin="ms-1" />
                        </Button>
                    )}
                </ModalFooter>
            </Form>
        </FormProvider>
    );
}

type Step = CreateDatabaseStep<FormData>;

function getActiveStepsList(
    formValues: FormData,
    errors: FieldErrors<FormData>,
    isValidatingDatabaseName: boolean
): Step[] {
    const {
        sourceStep: { isEncrypted },
    } = formValues;

    const allSteps: Step[] = [
        {
            id: "basicInfoStep",
            label: "Database type",
            active: true,
            isInvalid: !!errors.basicInfoStep,
            isLoading: isValidatingDatabaseName,
        },
        { id: "sourceStep", label: "Backup Source", active: true, isInvalid: !!errors.sourceStep },
        {
            id: "encryptionStep",
            label: "Encryption",
            active: isEncrypted,
            isInvalid: !!errors.encryptionStep,
        },
        {
            id: "dataDirectoryStep",
            label: "Data Directory",
            active: true,
            isInvalid: !!errors.dataDirectoryStep,
        },
    ];

    return allSteps.filter((step) => step.active);
}

function getStepViews(
    control: Control<FormData>,
    formValues: FormData,
    setValue: UseFormSetValue<FormData>,
    trigger: UseFormTrigger<FormData>
): Record<keyof FormData, JSX.Element> {
    const { encryptionKeyFileName, encryptionKeyText } = createDatabaseUtils.getEncryptionData(
        formValues.basicInfoStep.databaseName,
        formValues.encryptionStep.key
    );

    const sourceType = formValues.sourceStep.sourceType;
    const firstRestorePoint = formValues.sourceStep.sourceData[sourceType]?.pointsWithTags?.[0]?.restorePoint;
    const isEncryptionReadOnly =
        firstRestorePoint && firstRestorePoint.isEncrypted && firstRestorePoint.isSnapshotRestore;

    return {
        basicInfoStep: <StepBasicInfo />,
        sourceStep: <StepSource />,
        encryptionStep: (
            <StepEncryption
                control={control}
                encryptionKey={formValues.encryptionStep.key}
                fileName={encryptionKeyFileName}
                keyText={encryptionKeyText}
                setEncryptionKey={(x) => setValue("encryptionStep.key", x)}
                triggerEncryptionKey={() => trigger("encryptionStep.key")}
                encryptionKeyFieldName="encryptionStep.key"
                isSavedFieldName="encryptionStep.isKeySaved"
                isReadOnly={isEncryptionReadOnly}
            />
        ),
        dataDirectoryStep: <StepPath isBackupFolder manualSelectedNodes={null} />,
    };
}
