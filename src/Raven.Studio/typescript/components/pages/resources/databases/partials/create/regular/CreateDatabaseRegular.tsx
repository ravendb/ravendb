import React, { useState } from "react";
import { Button, CloseButton, Form, ModalBody, ModalFooter } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import Steps from "components/common/Steps";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    CreateDatabaseRegularFormData as FormData,
    createDatabaseRegularSchema,
} from "./createDatabaseRegularValidation";
import StepBasicInfo from "./steps/CreateDatabaseRegularStepBasicInfo";
import StepEncryption from "./steps/CreateDatabaseRegularStepEncryption";
import StepReplicationAndSharding from "./steps/CreateDatabaseRegularStepReplicationAndSharding";
import StepNodeSelection from "./steps/CreateDatabaseRegularStepNodeSelection";
import StepPaths from "../shared/CreateDatabaseStepPaths";
import { DevTool } from "@hookform/devtools";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { CreateDatabaseDto } from "commands/resources/createDatabaseCommand";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import databasesManager from "common/shell/databasesManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import QuickCreateButton from "components/pages/resources/databases/partials/create/regular/QuickCreateButton";
import { yupResolver } from "@hookform/resolvers/yup";
import { useDatabaseNameValidation } from "components/pages/resources/databases/partials/create/shared/useDatabaseNameValidation";

interface CreateDatabaseRegularProps {
    closeModal: () => void;
    changeCreateModeToBackup: () => void;
}

type StepId = "createNew" | "encryption" | "replicationAndSharding" | "nodeSelection" | "paths";

interface StepItem {
    id: StepId;
    label: string;
    active: boolean;
}

// TODO google events

export default function CreateDatabaseRegular({ closeModal, changeCreateModeToBackup }: CreateDatabaseRegularProps) {
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);
    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(allNodeTags.length),
        resolver: yupResolver(createDatabaseRegularSchema),
        context: {
            usedDatabaseNames,
        },
    });
    const { control, handleSubmit, formState, setError, clearErrors } = form;

    if (formState.errors) {
        console.log("kalczur errors", formState.errors);
    }
    const formValues = useWatch({
        control,
    });

    useDatabaseNameValidation(formValues.databaseName, setError, clearErrors);

    const { databasesService } = useServices();

    const asyncCreateDatabase = useAsyncCallback(
        async (dto: CreateDatabaseDto, replicationFactor: number) => {
            return databasesService.createDatabase(dto, replicationFactor);
        },
        {
            onSuccess: () => {
                closeModal();
            },
        }
    );

    const selectedOrchestrators =
        formValues.isSharded && formValues.isManualReplication ? formValues.manualNodes : allNodeTags;

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        tryHandleSubmit(async () => {
            databasesManager.default.activateAfterCreation(formValues.databaseName);

            const dto = mapToDto(formValues, selectedOrchestrators);
            await asyncCreateDatabase.execute(dto, formValues.replicationFactor);
        });
    };

    const [currentStep, setCurrentStep] = useState(0);

    const stepsList: StepItem[] = [
        { id: "createNew", label: "Name", active: true },
        {
            id: "encryption",
            label: "Encryption",
            active: formValues.isEncrypted,
        },
        {
            id: "replicationAndSharding",
            label: "Replication & Sharding",
            active: true,
        },
        {
            id: "nodeSelection",
            label: "Manual Node Selection",
            active: formValues.isManualReplication,
        },
        { id: "paths", label: "Paths Configuration", active: true },
    ];

    const activeSteps = stepsList.filter((step) => step.active);
    const isFirstStep = currentStep === 0;
    const isLastStep = currentStep === activeSteps.length - 1;

    const goToStep = (stepNum: number) => {
        setCurrentStep(stepNum);
    };

    const nextStep = () => {
        if (!isLastStep) {
            setCurrentStep((step) => step + 1);
        }
    };

    const prevStep = () => {
        if (!isFirstStep) {
            setCurrentStep((step) => step - 1);
        }
    };

    const stepViews: Record<StepId, JSX.Element> = {
        createNew: <StepBasicInfo />,
        encryption: <StepEncryption />,
        replicationAndSharding: <StepReplicationAndSharding />,
        nodeSelection: <StepNodeSelection />,
        paths: (
            <StepPaths
                databaseName={formValues.databaseName}
                manualSelectedNodes={formValues.isManualReplication ? formValues.manualNodes : null}
                isBackupFolder={false}
            />
        ),
    };

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <DevTool control={control} />
                <ModalBody>
                    <div className="d-flex  mb-5">
                        <Steps
                            current={currentStep}
                            steps={activeSteps.map((step) => step.label)}
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
                        <Button type="button" onClick={changeCreateModeToBackup} className="rounded-pill">
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
        replicationFactor,
        databaseName: "",
        isEncrypted: false,
        isEncryptionKeySaved: false,
        isSharded: false,
        shardsCount: 1,
        isDynamicDistribution: false,
        isManualReplication: false,
        manualNodes: [],
        manualShard: [],
        isPathDefault: true,
        path: "",
    };
};

function mapToDto(formValues: FormData, selectedOrchestrators: string[]): CreateDatabaseDto {
    const {
        databaseName,
        isSharded,
        isManualReplication,
        manualNodes,
        manualShard,
        shardsCount,
        isPathDefault,
        path,
        isEncrypted,
        isDynamicDistribution,
    } = formValues;

    const Settings: CreateDatabaseDto["Settings"] = isPathDefault
        ? {}
        : {
              DataDir: _.trim(path),
          };

    const Topology: CreateDatabaseDto["Topology"] = isSharded
        ? null
        : {
              Members: isManualReplication ? manualNodes : null,
              DynamicNodesDistribution: isDynamicDistribution,
          };

    const Shards: CreateDatabaseDto["Sharding"]["Shards"] = {};

    if (isSharded) {
        for (let i = 0; i < shardsCount; i++) {
            Shards[i] = isManualReplication
                ? {
                      Members: manualShard[i],
                  }
                : {};
        }
    }

    const Sharding: CreateDatabaseDto["Sharding"] = isSharded
        ? {
              Shards,
              Orchestrator: {
                  Topology: {
                      Members: selectedOrchestrators,
                  },
              },
          }
        : null;

    return {
        DatabaseName: databaseName,
        Settings,
        Disabled: false,
        Encrypted: isEncrypted,
        Topology,
        Sharding,
    };
}
