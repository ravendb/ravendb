import React, { useCallback } from "react";
import Form from "react-bootstrap/Form";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import Steps from "components/common/steps/Steps";
import {
    CreateDatabaseRegularFormData as FormData,
    createDatabaseRegularSchema,
} from "./createDatabaseRegularValidation";
import StepBasicInfo from "./steps/CreateDatabaseRegularStepBasicInfo";
import StepEncryption from "../../../../../../common/FormEncryption";
import StepReplicationAndSharding from "./steps/CreateDatabaseRegularStepReplicationAndSharding";
import StepNodeSelection from "./steps/CreateDatabaseRegularStepNodeSelection";
import StepShardingPrefixes from "components/pages/resources/databases/partials/create/regular/steps/StepShardingPrefixes";
import StepPath from "../shared/CreateDatabaseStepDataDirectory";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import databasesManager from "common/shell/databasesManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import QuickCreateButton from "./QuickCreateButton";
import { yupResolver } from "@hookform/resolvers/yup";
import {
    Control,
    FieldErrors,
    FormProvider,
    SubmitHandler,
    useForm,
    UseFormSetValue,
    UseFormTrigger,
    useWatch,
} from "react-hook-form";
import { useSteps } from "components/common/steps/useSteps";
import { useCreateDatabaseAsyncValidation } from "../shared/useCreateDatabaseAsyncValidation";
import { createDatabaseRegularDataUtils } from "./createDatabaseRegularDataUtils";
import { CreateDatabaseStep, createDatabaseUtils } from "../shared/createDatabaseUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useCreateDatabaseShortcuts } from "../shared/useCreateDatabaseShortcuts";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";

interface CreateDatabaseRegularProps {
    closeModal: () => void;
    changeCreateModeToBackup: () => void;
}

export default function CreateDatabaseRegular({ closeModal, changeCreateModeToBackup }: CreateDatabaseRegularProps) {
    const { databasesService } = useServices();
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);
    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const form = useForm<FormData>({
        mode: "onChange",
        defaultValues: createDatabaseRegularDataUtils.getDefaultValues(allNodeTags),
        resolver: (data, _, options) =>
            yupResolver(createDatabaseRegularSchema)(
                data,
                {
                    usedDatabaseNames,
                    isSharded: data.replicationAndShardingStep.isSharded,
                    isManualReplication: data.replicationAndShardingStep.isManualReplication,
                    isEncrypted: data.basicInfoStep.isEncrypted,
                    isPrefixesForShards: data.replicationAndShardingStep.isPrefixesForShards,
                    usedPrefixes: data.shardingPrefixesStep.prefixesForShards.map((x) => x.prefix),
                },
                options
            ),
    });

    const { control, handleSubmit, formState, setValue, setError, trigger } = form;
    const formValues = useWatch({
        control,
    });

    const asyncDatabaseNameValidation = useCreateDatabaseAsyncValidation(
        formValues.basicInfoStep.databaseName.trim(),
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
            });
        },
        [activeSteps, asyncDatabaseNameValidation, currentStep, trigger]
    );

    const { reportEvent } = useEventsCollector();

    const onFinish: SubmitHandler<FormData> = useCallback(
        async (formValues) => {
            return tryHandleSubmit(async () => {
                reportEvent("database", "newDatabase");

                if (formValues.basicInfoStep.isEncrypted) {
                    const nodes = formValues.replicationAndShardingStep.isSharded
                        ? Array.from(new Set(formValues.manualNodeSelectionStep.shards.flat()))
                        : formValues.manualNodeSelectionStep.nodes;

                    await databasesService.distributeSecret(
                        formValues.basicInfoStep.databaseName,
                        formValues.encryptionStep.key,
                        nodes
                    );
                }

                databasesManager.default.activateAfterCreation(formValues.basicInfoStep.databaseName);

                const dto = createDatabaseRegularDataUtils.mapToDto(formValues, allNodeTags);
                await databasesService.createDatabase(dto, formValues.replicationAndShardingStep.replicationFactor);

                closeModal();
            });
        },
        [allNodeTags, databasesService, reportEvent, closeModal]
    );

    const handleGoNext = useCallback(async () => {
        await nextStepWithValidation(validateToTargetStep(currentStep));
    }, [currentStep, nextStepWithValidation, validateToTargetStep]);

    const handleQuickCreate = useCallback(async () => {
        if (activeSteps[currentStep].id === "basicInfoStep") {
            const isNameValid = await asyncDatabaseNameValidation.execute();
            if (!isNameValid) {
                return;
            }
        }

        await handleSubmit(onFinish)();
    }, [activeSteps, asyncDatabaseNameValidation, currentStep, handleSubmit, onFinish]);

    useCreateDatabaseShortcuts({
        submit: handleQuickCreate,
        handleGoNext,
        isLastStep,
        canQuickCreate: true,
    });

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <Modal.Header className="d-flex" onCloseClick={closeModal}>
                    <Steps
                        current={currentStep}
                        steps={activeSteps.map(createDatabaseUtils.mapToStepItem)}
                        onClick={(step) => goToStepWithValidation(step, validateToTargetStep(step - 1))}
                        className="flex-grow me-4"
                    ></Steps>
                </Modal.Header>
                <Modal.Body>{stepViews[activeSteps[currentStep].id]}</Modal.Body>
                <hr />
                <Modal.Footer>
                    {isFirstStep ? (
                        <Button
                            type="button"
                            onClick={changeCreateModeToBackup}
                            className="rounded-pill"
                            variant="secondary"
                            disabled={formState.isSubmitting}
                        >
                            <Icon icon="database" addon="arrow-up" /> Restore from backup
                        </Button>
                    ) : (
                        <Button variant="secondary" type="button" onClick={prevStep} className="rounded-pill">
                            <Icon icon="arrow-thin-left" /> Back
                        </Button>
                    )}
                    <FlexGrow />
                    {!isLastStep && (
                        <QuickCreateButton
                            formValues={formValues}
                            isSubmitting={formState.isSubmitting}
                            handleQuickCreate={handleQuickCreate}
                        />
                    )}
                    {isLastStep ? (
                        <ButtonWithSpinner
                            type="submit"
                            variant="success"
                            className="rounded-pill"
                            icon="rocket"
                            isSpinning={formState.isSubmitting}
                        >
                            Finish
                        </ButtonWithSpinner>
                    ) : (
                        <Button type="button" variant="primary" className="rounded-pill" onClick={handleGoNext}>
                            Next <Icon icon="arrow-thin-right" margin="ms-1" />
                        </Button>
                    )}
                </Modal.Footer>
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
    const steps: Step[] = [
        {
            id: "basicInfoStep",
            label: "Database Name",
            active: true,
            isInvalid: !!errors.basicInfoStep,
            isLoading: isValidatingDatabaseName,
        },
        {
            id: "encryptionStep",
            label: "Encryption",
            active: formValues.basicInfoStep.isEncrypted,
            isInvalid: !!errors.encryptionStep,
        },
        {
            id: "replicationAndShardingStep",
            label: "Replication & Sharding",
            active: true,
            isInvalid: !!errors.replicationAndShardingStep,
        },
        {
            id: "manualNodeSelectionStep",
            label: "Manual Node Selection",
            active: formValues.replicationAndShardingStep.isManualReplication,
            isInvalid: !!errors.manualNodeSelectionStep,
        },
        {
            id: "shardingPrefixesStep",
            label: "Sharding Prefixes",
            active: formValues.replicationAndShardingStep.isPrefixesForShards,
            isInvalid: !!errors.shardingPrefixesStep,
        },
        {
            id: "dataDirectoryStep",
            label: "Data Directory",
            active: true,
            isInvalid: !!errors.dataDirectoryStep,
        },
    ];

    return steps.filter((step) => step.active);
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

    return {
        basicInfoStep: <StepBasicInfo />,
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
            />
        ),
        replicationAndShardingStep: <StepReplicationAndSharding />,
        manualNodeSelectionStep: <StepNodeSelection />,
        shardingPrefixesStep: <StepShardingPrefixes />,
        dataDirectoryStep: (
            <StepPath
                isBackupFolder={false}
                manualSelectedNodes={
                    formValues.replicationAndShardingStep.isManualReplication
                        ? formValues.manualNodeSelectionStep.nodes
                        : null
                }
            />
        ),
    };
}
