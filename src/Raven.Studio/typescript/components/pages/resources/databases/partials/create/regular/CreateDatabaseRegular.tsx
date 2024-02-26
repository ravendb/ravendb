import React from "react";
import { Button, CloseButton, Form, ModalBody, ModalFooter } from "reactstrap";
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
import StepPath from "../shared/CreateDatabaseStepPath";
import { DevTool } from "@hookform/devtools";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

import { useServices } from "components/hooks/useServices";
import databasesManager from "common/shell/databasesManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import QuickCreateButton from "components/pages/resources/databases/partials/create/regular/QuickCreateButton";
import { yupResolver } from "@hookform/resolvers/yup";
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
import { useSteps } from "components/common/steps/useSteps";
import { useCreateDatabaseAsyncValidation } from "components/pages/resources/databases/partials/create/shared/useCreateDatabaseAsyncValidation";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";
import {
    CreateDatabaseStep,
    createDatabaseUtils,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";

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
        defaultValues: createDatabaseRegularDataUtils.getDefaultValues(allNodeTags.length),
        resolver: (data, _, options) =>
            yupResolver(createDatabaseRegularSchema)(
                data,
                {
                    usedDatabaseNames,
                    isSharded: data.replicationAndSharding.isSharded,
                    isManualReplication: data.replicationAndSharding.isManualReplication,
                    isEncrypted: data.basicInfo.isEncrypted,
                },
                options
            ),
    });

    const { control, handleSubmit, formState, setValue, setError, trigger } = form;
    const formValues = useWatch({
        control,
    });
    console.log("kalczur Regular errors", formState.errors); // TODO remove

    const activeSteps = getActiveStepsList(formValues, formState);
    const { currentStep, isFirstStep, isLastStep, goToStepWithValidation, nextStepWithValidation, prevStep } = useSteps(
        activeSteps.length
    );
    const stepViews = getStepViews(control, formValues, setValue, trigger);

    const asyncDatabaseNameValidation = useCreateDatabaseAsyncValidation(formValues.basicInfo.databaseName, setError);

    const stepValidation = createDatabaseUtils.getStepValidation(
        activeSteps[currentStep].id,
        trigger,
        asyncDatabaseNameValidation,
        formValues.basicInfo.databaseName
    );

    const { reportEvent } = useEventsCollector();

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            reportEvent("database", "newDatabase");

            asyncDatabaseNameValidation.execute(formValues.basicInfo.databaseName);
            if (!asyncDatabaseNameValidation.result) {
                return;
            }

            databasesManager.default.activateAfterCreation(formValues.basicInfo.databaseName);

            const dto = createDatabaseRegularDataUtils.mapToDto(formValues, allNodeTags);
            await databasesService.createDatabase(dto, formValues.replicationAndSharding.replicationFactor);

            closeModal();
        });
    };

    // TODO add step validation spinner

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <ModalBody>
                    <DevTool control={control} />
                    <div className="d-flex  mb-5">
                        <Steps
                            current={currentStep}
                            steps={activeSteps.map((step) => ({
                                label: step.label,
                                isInvalid: step.isInvalid,
                            }))}
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
                            onClick={changeCreateModeToBackup}
                            className="rounded-pill"
                            disabled={formState.isSubmitting}
                        >
                            <Icon icon="database" addon="arrow-up" /> Create from backup
                        </Button>
                    ) : (
                        <Button type="button" onClick={prevStep} className="rounded-pill">
                            <Icon icon="arrow-thin-left" /> Back
                        </Button>
                    )}
                    <FlexGrow />
                    {!isLastStep && <QuickCreateButton formValues={formValues} isSubmitting={formState.isSubmitting} />}
                    {isLastStep ? (
                        <ButtonWithSpinner
                            type="submit"
                            color="success"
                            className="rounded-pill"
                            icon="rocket"
                            isSpinning={formState.isSubmitting}
                        >
                            Finish
                        </ButtonWithSpinner>
                    ) : (
                        <Button
                            type="button"
                            color="primary"
                            className="rounded-pill"
                            onClick={() => nextStepWithValidation(stepValidation)}
                            disabled={isLastStep}
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

function getActiveStepsList(formValues: FormData, formState: FormState<FormData>): Step[] {
    const steps: Step[] = [
        { id: "basicInfo", label: "Name", active: true, isInvalid: !!formState.errors.basicInfo },
        {
            id: "encryption",
            label: "Encryption",
            active: formValues.basicInfo.isEncrypted,
            isInvalid: !!formState.errors.encryption,
        },
        {
            id: "replicationAndSharding",
            label: "Replication & Sharding",
            active: true,
            isInvalid: !!formState.errors.replicationAndSharding,
        },
        {
            id: "manualNodeSelection",
            label: "Manual Node Selection",
            active: formValues.replicationAndSharding.isManualReplication,
            isInvalid: !!formState.errors.manualNodeSelection,
        },
        {
            id: "pathsConfigurations",
            label: "Paths Configuration",
            active: true,
            isInvalid: !!formState.errors.pathsConfigurations,
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
        formValues.basicInfo.databaseName,
        formValues.encryption.key
    );

    return {
        basicInfo: <StepBasicInfo />,
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
        replicationAndSharding: <StepReplicationAndSharding />,
        manualNodeSelection: <StepNodeSelection />,
        pathsConfigurations: (
            <StepPath
                isBackupFolder={false}
                manualSelectedNodes={
                    formValues.replicationAndSharding.isManualReplication ? formValues.manualNodeSelection.nodes : null
                }
            />
        ),
    };
}
