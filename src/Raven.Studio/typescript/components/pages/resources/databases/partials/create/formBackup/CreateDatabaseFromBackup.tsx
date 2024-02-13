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
import StepSource from "./steps/CreateDatabaseFromBackupStepSource";
import { tryHandleSubmit } from "components/utils/common";
import { DevTool } from "@hookform/devtools";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import databasesManager from "common/shell/databasesManager";
import { useCreateDatabase } from "components/pages/resources/databases/partials/create/shared/useCreateDatabase";
import { useSteps } from "components/common/steps/useSteps";

interface CreateDatabaseFromBackupProps {
    closeModal: () => void;
    changeCreateModeToRegular: () => void;
}

type StepId = "basicInfo" | "backupSource" | "encryption" | "path";

// TODO move to shared
interface StepsListItem {
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
        mode: "all",
        defaultValues,
        resolver: (data, _, options) =>
            yupResolver(createDatabaseFromBackupSchema)(
                data,
                {
                    usedDatabaseNames: usedDatabaseNames,
                    sourceType: data.source.sourceType,
                    isEncrypted: data.source.isEncrypted,
                },
                options
            ),
    });

    const { control, setError, handleSubmit, formState, setValue, trigger } = form;
    const formValues = useWatch({
        control,
    });

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
            await databasesService.restoreDatabaseFromBackup(mapToDto(formValues));

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
                            steps={activeSteps.map((step) => ({ label: step.label }))}
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

const defaultRestorePoints: TODO = [
    {
        restorePoint: null,
        nodeTag: "",
    },
];

const defaultValues: FormData = {
    basicInfo: {
        databaseName: "",
        isSharded: false,
    },
    source: {
        isDisableOngoingTasksAfterRestore: false,
        isSkipIndexes: false,
        isEncrypted: false,
        sourceType: "local",
        sourceData: {
            local: {
                directory: "",
                restorePoints: defaultRestorePoints,
            },
            cloud: {
                link: "",
                restorePoints: [],
            },
            amazonS3: {
                isUseCustomHost: false,
                isForcePathStyle: false,
                customHost: "",
                accessKey: "",
                secretKey: "",
                awsRegion: "",
                bucketName: "",
                remoteFolderName: "",
                restorePoints: [],
            },
            azure: {
                accountKey: "",
                accountName: "",
                container: "",
                remoteFolderName: "",
                restorePoints: [],
            },
            googleCloud: {
                bucketName: "",
                credentialsJson: "",
                remoteFolderName: "",
                restorePoints: [],
            },
        },
    },
    encryption: {
        isKeySaved: false,
        key: "",
    },
    pathsConfigurations: {
        isDefault: true,
        path: "",
    },
};

// TODO rename path to dataDirectory?

function mapToDto({ basicInfo, source, encryption, pathsConfigurations }: FormData): TODO {
    return {
        DatabaseName: basicInfo.databaseName,
        DisableOngoingTasks: source.isDisableOngoingTasksAfterRestore,
        SkipIndexes: source.isSkipIndexes,
        DataDirectory: _.trim(pathsConfigurations.path),
        EncryptionKey: encryption.key, // todo conditional
        BackupEncryptionSettings: null,
        Type: source.sourceType,
    };
}

// function getS3Settings(formValues: FormData): Raven.Client.Documents.Operations.Backups.S3Settings {
//     return {
//         AwsAccessKey: formValues.awsAccessKey,
//         AwsSecretKey: formValues.awsSecretKey,
//         AwsRegionName: "us-east-1",
//         BucketName: "damian-test-backup",
//         AwsSessionToken: "",
//         RemoteFolderName: null,
//         Disabled: false,
//         GetBackupConfigurationScript: null,
//         CustomServerUrl: null,
//         ForcePathStyle: false,
//     };
// }

// function getRestorePointsData(formValues: FormData): TODO {
//     let restorePoints: TODO[] = [];

//     if (formValues.source === "local") {
//         restorePoints = formValues.localRestorePoints;
//     } else if (formValues.source === "ravenCloud") {
//         restorePoints = formValues.ravenCloudRestorePoints;
//     } else if (formValues.source === "aws") {
//         restorePoints = formValues.awsRestorePoints;
//     } else if (formValues.source === "azure") {
//         restorePoints = formValues.azureRestorePoints;
//     } else if (formValues.source === "gcp") {
//         restorePoints = formValues.gcpRestorePoints;
//     }

//     if (formValues.isSharded) {
//         return {
//             ShardRestoreSettings: {
//                 Shards: restorePoints.reduce((acc, restorePoint, index) => {
//                     acc[index] = {
//                         FolderName: restorePoint.restorePoint.location,
//                         LastFileNameToRestore: restorePoint.restorePoint.fileName,
//                         NodeTag: restorePoint.nodeTag,
//                         ShardNumber: index,
//                     };
//                     return acc;
//                 }, {}),
//             },
//             LastFileNameToRestore: null,
//             BackupLocation: null,
//         };
//     } else {
//         return {
//             LastFileNameToRestore: restorePoints[0].restorePoint.fileName,
//             BackupLocation: restorePoints[0].restorePoint.location,
//             ShardRestoreSettings: null,
//         };
//     }
// }

function getActiveStepsList(formValues: FormData, formState: FormState<FormData>): StepsListItem[] {
    const allSteps: StepsListItem[] = [
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
