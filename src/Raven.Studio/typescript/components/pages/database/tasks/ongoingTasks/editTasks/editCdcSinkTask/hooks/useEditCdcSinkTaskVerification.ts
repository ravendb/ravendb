import messagePublisher from "common/messagePublisher";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { mapSqlConnectionsFromDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto";
import { mapSqlConnectionStringToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useEditCdcSinkTaskRawViewSync } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskRawViewSync";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import isEqual from "lodash/isEqual";
import { useRef } from "react";
import { useAsyncCallback } from "react-async-hook";
import { UseFormReturn, useWatch } from "react-hook-form";
import CdcTestResult = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestResult;

const verifiedFields = ["connectionStringName", "postgresPublicationName", "postgresSlotName", "tables"] as const;

type VerifiedInputs = unknown[];

interface Verification {
    inputs: VerifiedInputs;
    result: CdcTestResult;
}

export interface EditCdcSinkTaskVerification {
    execute: () => Promise<CdcTestResult>;
    verify: (formData: EditCdcSinkTaskFormData) => Promise<CdcTestResult>;
    getCurrentResult: (formData: EditCdcSinkTaskFormData) => CdcTestResult;
    loading: boolean;
    error: Error;
    result: CdcTestResult;
}

function pickVerifiedInputs(formData: EditCdcSinkTaskFormData): VerifiedInputs {
    return verifiedFields.map((field) => structuredClone(formData[field]));
}

export function useEditCdcSinkTaskVerification(
    editForm: UseFormReturn<EditCdcSinkTaskFormData>
): EditCdcSinkTaskVerification {
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const taskId = useAppSelector(editCdcSinkTaskSelectors.taskId);
    const sqlConnections = useAppSelector(connectionStringSelectors.connectionsByType("Sql"));
    const { control, getValues, trigger } = editForm;
    const applyRawViewContent = useEditCdcSinkTaskRawViewSync(editForm);

    const lastVerification = useRef<Verification>(null);

    const findSqlConnection = async (name: string) => {
        const connection = sqlConnections.find((x) => x.name === name);
        if (connection) {
            return connection;
        }

        const connectionStringsDto = await tasksService.getConnectionStrings(databaseName);
        return mapSqlConnectionsFromDto(connectionStringsDto.SqlConnectionStrings).find((x) => x.name === name);
    };

    const asyncVerify = useAsyncCallback(async (formData: EditCdcSinkTaskFormData) => {
        const connection = await findSqlConnection(formData.connectionStringName);
        if (!connection) {
            messagePublisher.reportError(`Connection string '${formData.connectionStringName}' was not found.`);
            return null;
        }

        const inputs = pickVerifiedInputs(formData);
        lastVerification.current = { inputs, result: null };

        const result = await tasksService.verifyCdcSink(databaseName, {
            Configuration: editCdcSinkTaskUtils.mapToDto(formData, taskId),
            Connection: mapSqlConnectionStringToDto(connection),
        });

        lastVerification.current = { inputs, result };
        return result;
    });

    const execute = async () => {
        if (!applyRawViewContent()) {
            return null;
        }

        const isValid = await trigger();
        if (!isValid) {
            return null;
        }

        return await asyncVerify.execute(getValues());
    };

    const isCurrent = (inputs: VerifiedInputs) =>
        lastVerification.current != null && isEqual(lastVerification.current.inputs, inputs);

    const getCurrentResult = (formData: EditCdcSinkTaskFormData): CdcTestResult =>
        isCurrent(pickVerifiedInputs(formData)) ? lastVerification.current.result : null;

    const isWatchedCurrent = isCurrent(useWatch({ control, name: verifiedFields }));

    return {
        execute,
        verify: asyncVerify.execute,
        getCurrentResult,
        loading: asyncVerify.loading,
        error: isWatchedCurrent ? asyncVerify.error : null,
        result: isWatchedCurrent ? lastVerification.current.result : null,
    };
}
