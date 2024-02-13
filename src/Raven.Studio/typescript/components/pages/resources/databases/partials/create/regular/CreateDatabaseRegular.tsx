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
import { CreateDatabaseDto } from "commands/resources/createDatabaseCommand";
import { useServices } from "components/hooks/useServices";
import databasesManager from "common/shell/databasesManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import QuickCreateButton from "components/pages/resources/databases/partials/create/regular/QuickCreateButton";
import { yupResolver } from "@hookform/resolvers/yup";
import { useDatabaseNameValidation } from "components/pages/resources/databases/partials/create/shared/useDatabaseNameValidation";
import { FormProvider, FormState, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useCreateDatabase } from "components/pages/resources/databases/partials/create/shared/useCreateDatabase";
import { useSteps } from "components/common/steps/useSteps";

interface CreateDatabaseRegularProps {
    closeModal: () => void;
    changeCreateModeToBackup: () => void;
}

type StepId = "createNew" | "encryption" | "replicationAndSharding" | "nodeSelection" | "path";

interface StepsListItem {
    id: StepId;
    label: string;
    active: boolean;
    isInvalid?: boolean;
}

// TODO google events

export default function CreateDatabaseRegular({ closeModal, changeCreateModeToBackup }: CreateDatabaseRegularProps) {
    const { databasesService } = useServices();
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);
    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(allNodeTags.length),
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

    const { control, handleSubmit, formState, setError, clearErrors, setValue, trigger } = form;
    const formValues = useWatch({
        control,
    });

    useDatabaseNameValidation(formValues.basicInfo.databaseName, setError, clearErrors);

    const { encryptionKeyFileName, encryptionKeyText } = useCreateDatabase(formValues);
    const activeSteps = getActiveStepsList(formValues, formState);
    const { currentStep, isFirstStep, isLastStep, goToStep, nextStep, prevStep } = useSteps(activeSteps.length);

    // TODO to function
    const stepViews: Record<StepId, JSX.Element> = {
        createNew: <StepBasicInfo />,
        encryption: (
            <StepEncryption
                control={control}
                encryptionKey={formValues.encryption.key}
                fileName={encryptionKeyFileName}
                keyText={encryptionKeyText}
                setValue={setValue}
                triggerDatabaseName={() => trigger("basicInfo.databaseName")}
                triggerEncryptionKey={() => trigger("encryption.key")}
                encryptionKeyFieldName="encryption.key"
                isSavedFieldName="encryption.isKeySaved"
            />
        ),
        replicationAndSharding: <StepReplicationAndSharding />,
        nodeSelection: <StepNodeSelection />,
        path: (
            <StepPath
                isBackupFolder={false}
                manualSelectedNodes={
                    formValues.replicationAndSharding.isManualReplication ? formValues.manualNodeSelection.nodes : null
                }
            />
        ),
    };

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            databasesManager.default.activateAfterCreation(formValues.basicInfo.databaseName);

            const dto = mapToDto(formValues, allNodeTags);
            await databasesService.createDatabase(dto, formValues.replicationAndSharding.replicationFactor);
            closeModal();
        });
    };

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
                            onClick={nextStep}
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

const getDefaultValues = (replicationFactor: number): FormData => {
    return {
        basicInfo: {
            databaseName: "",
            isEncrypted: false,
        },
        encryption: {
            key: "",
            isKeySaved: false,
        },
        replicationAndSharding: {
            replicationFactor,
            isSharded: false,
            shardsCount: 1,
            isDynamicDistribution: false,
            isManualReplication: false,
        },
        manualNodeSelection: {
            nodes: [],
            shards: [],
        },
        pathsConfigurations: {
            isDefault: true,
            path: "",
        },
    };
};

function mapToDto(formValues: FormData, allNodeTags: string[]): CreateDatabaseDto {
    const { basicInfo, replicationAndSharding, manualNodeSelection, pathsConfigurations } = formValues;

    const Settings: CreateDatabaseDto["Settings"] = pathsConfigurations.isDefault
        ? {}
        : {
              DataDir: _.trim(pathsConfigurations.path),
          };

    const Topology: CreateDatabaseDto["Topology"] = replicationAndSharding.isSharded
        ? null
        : {
              Members: replicationAndSharding.isManualReplication ? manualNodeSelection.nodes : null,
              DynamicNodesDistribution: replicationAndSharding.isDynamicDistribution,
          };

    const Shards: CreateDatabaseDto["Sharding"]["Shards"] = {};

    if (replicationAndSharding.isSharded) {
        for (let i = 0; i < replicationAndSharding.shardsCount; i++) {
            Shards[i] = replicationAndSharding.isManualReplication
                ? {
                      Members: manualNodeSelection.shards[i],
                  }
                : {};
        }
    }

    const selectedOrchestrators =
        formValues.replicationAndSharding.isSharded && formValues.replicationAndSharding.isManualReplication
            ? formValues.manualNodeSelection.nodes
            : allNodeTags;

    const Sharding: CreateDatabaseDto["Sharding"] = replicationAndSharding.isSharded
        ? {
              Shards,
              Orchestrator: {
                  Topology: {
                      Members: selectedOrchestrators,
                  },
              },
          }
        : null;

    // TODO encription key?

    return {
        DatabaseName: basicInfo.databaseName,
        Settings,
        Disabled: false,
        Encrypted: basicInfo.isEncrypted,
        Topology,
        Sharding,
    };
}

function getActiveStepsList(formValues: FormData, formState: FormState<FormData>): StepsListItem[] {
    const steps: StepsListItem[] = [
        { id: "createNew", label: "Name", active: true, isInvalid: !!formState.errors.basicInfo },
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
            id: "nodeSelection",
            label: "Manual Node Selection",
            active: formValues.replicationAndSharding.isManualReplication,
            isInvalid: !!formState.errors.manualNodeSelection,
        },
        { id: "path", label: "Paths Configuration", active: true, isInvalid: !!formState.errors.pathsConfigurations },
    ];

    return steps.filter((step) => step.active);
}
