import { StepItem } from "components/common/steps/Steps";
import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";
import moment from "moment";
import { UseAsyncReturn } from "react-async-hook";
import { Path, UseFormTrigger } from "react-hook-form";

function getEncryptionData(databaseName: string, encryptionKey: string) {
    const encryptionKeyFileName = `Key-of-${databaseName}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;

    const encryptionKeyText = `Encryption Key for database '${databaseName}': ${encryptionKey}\r\n\r\nThis key is used to encrypt the RavenDB database, it is required for restoring the database.\r\nMake sure you keep it in a private, safe place as it will Not be available again !`;

    return {
        encryptionKeyFileName,
        encryptionKeyText,
    };
}

type FormData = CreateDatabaseFromBackupFormData | CreateDatabaseRegularFormData;

function getStepValidation(
    stepId: Path<FormData>,
    trigger: UseFormTrigger<FormData>,
    asyncDatabaseNameValidation: UseAsyncReturn<boolean, [string]>,
    databaseName: string
) {
    if (stepId === "basicInfoStep") {
        return async () => {
            const basicInfoResult = await trigger(stepId);
            asyncDatabaseNameValidation.execute(databaseName);
            return asyncDatabaseNameValidation.result && basicInfoResult;
        };
    }

    return async () => await trigger(stepId);
}

export interface CreateDatabaseStep<T extends FormData> {
    id: keyof T;
    label: string;
    active: boolean;
    isInvalid?: boolean;
    isLoading?: boolean;
}

function mapToStepItem<T extends FormData>(step: CreateDatabaseStep<T>): StepItem {
    return {
        label: step.label,
        isInvalid: step.isInvalid,
        isLoading: step.isLoading,
    };
}

export const createDatabaseUtils = {
    getEncryptionData,
    getStepValidation,
    mapToStepItem,
};
