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
import {
    Control,
    FormProvider,
    FormState,
    SubmitHandler,
    UseFormSetValue,
    UseFormTrigger,
    useForm,
    useWatch,
} from "react-hook-form";
import StepBasicInfo from "./steps/CreateDatabaseFromBackupStepBasicInfo";
import StepPath from "../shared/CreateDatabaseStepPath";
import StepEncryption from "../../../../../../common/FormEncryption";
import StepSource from "./steps/source/CreateDatabaseFromBackupStepSource";
import { tryHandleSubmit } from "components/utils/common";
import { DevTool } from "@hookform/devtools";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import databasesManager from "common/shell/databasesManager";
import { useSteps } from "components/common/steps/useSteps";
import { createDatabaseFromBackupDataUtils } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupDataUtils";
import notificationCenter from "common/notifications/notificationCenter";
import {
    CreateDatabaseStep,
    createDatabaseUtils,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";

interface CreateDatabaseFromBackupProps {
    closeModal: () => void;
    changeCreateModeToRegular: () => void;
}

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
    console.log("kalczur FromBackup errors", formState.errors); // TODO remove

    const asyncDatabaseNameValidation = useCreateDatabaseAsyncValidation(formValues.basicInfo.databaseName, setError);

    const activeSteps = getActiveStepsList(formValues, formState, asyncDatabaseNameValidation.loading);
    const { currentStep, isFirstStep, isLastStep, goToStepWithValidation, nextStepWithValidation, prevStep } = useSteps(
        activeSteps.length
    );
    const stepViews = getStepViews(control, formValues, setValue, trigger);

    const stepValidation = createDatabaseUtils.getStepValidation(
        activeSteps[currentStep].id,
        trigger,
        asyncDatabaseNameValidation,
        formValues.basicInfo.databaseName
    );

    const { reportEvent } = useEventsCollector();

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            reportEvent("database", "restore");

            asyncDatabaseNameValidation.execute(formValues.basicInfo.databaseName);
            if (!asyncDatabaseNameValidation.result) {
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
                            steps={activeSteps.map(createDatabaseUtils.mapToStepItem)}
                            onClick={(step) => goToStepWithValidation(step, stepValidation)}
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
                        <Button
                            type="button"
                            color="primary"
                            className="rounded-pill"
                            onClick={() => nextStepWithValidation(stepValidation)}
                            disabled={asyncDatabaseNameValidation.loading}
                        >
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
    formState: FormState<FormData>,
    isValidatingDatabaseName: boolean
): Step[] {
    const allSteps: Step[] = [
        {
            id: "basicInfo",
            label: "Select backup",
            active: true,
            isInvalid: !!formState.errors.basicInfo,
            isLoading: isValidatingDatabaseName,
        },
        { id: "source", label: "Backup Source", active: true, isInvalid: !!formState.errors.source },
        {
            id: "encryption",
            label: "Encryption",
            active: formValues.source.isEncrypted,
            isInvalid: !!formState.errors.encryption,
        },
        {
            id: "pathsConfigurations",
            label: "Paths Configuration",
            active: true,
            isInvalid: !!formState.errors.pathsConfigurations,
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
        formValues.basicInfo.databaseName,
        formValues.encryption.key
    );

    return {
        basicInfo: <StepBasicInfo />,
        source: <StepSource />,
        encryption: (
            <StepEncryption
                control={control}
                encryptionKey={formValues.encryption.key}
                fileName={encryptionKeyFileName}
                keyText={encryptionKeyText}
                setEncryptionKey={(x) => setValue("encryption.key", x)}
                triggerEncryptionKey={() => trigger("encryption.key")}
                encryptionKeyFieldName="encryption.key"
                isSavedFieldName="encryption.isKeySaved"
            />
        ),
        pathsConfigurations: <StepPath isBackupFolder manualSelectedNodes={null} />,
    };
}
