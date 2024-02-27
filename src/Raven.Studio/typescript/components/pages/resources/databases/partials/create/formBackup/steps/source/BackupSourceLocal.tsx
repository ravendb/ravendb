import { FormSelectAutocomplete } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { useServices } from "components/hooks/useServices";
import React from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Col, Row } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { mapRestorePointDtoToSelectOptions } from "components/pages/resources/databases/partials/create/formBackup/steps/source/backupSourceUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceLocal() {
    const { resourcesService } = useServices();
    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { local: localSourceData },
        },
    } = useWatch({
        control,
    });

    const asyncGetLocalFolderPathOptions = useAsyncDebounce(
        async (directory) => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(directory, true);

            return dto.List.map((x) => ({ value: x, label: x }) satisfies SelectOption);
        },
        [localSourceData.directory]
    );

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <label className="col-form-label">Directory Path</label>
                </Col>
                <Col>
                    <FormSelectAutocomplete
                        control={control}
                        name="sourceStep.sourceData.local.directory"
                        options={asyncGetLocalFolderPathOptions.result || []}
                        isLoading={asyncGetLocalFolderPathOptions.loading}
                        placeholder="Enter backup directory path"
                    />
                </Col>
            </Row>
            <RestorePointsFields
                isSharded={isSharded}
                restorePointsFieldName="sourceStep.sourceData.googleCloud.restorePoints"
                mapRestorePoint={(field, index) => (
                    <SourceRestorePoint
                        key={field.id}
                        index={index}
                        isSharded={isSharded}
                        directory={localSourceData.directory}
                    />
                )}
            />
            <EncryptionField
                encryptionKeyFieldName="sourceStep.sourceData.local.encryptionKey"
                selectedSourceData={localSourceData}
            />
        </>
    );
}

interface SourceRestorePointProps {
    index: number;
    isSharded: boolean;
    directory: string;
}

function SourceRestorePoint({ index, directory, isSharded }: SourceRestorePointProps) {
    const { control } = useFormContext<FormData>();

    const { remove } = useFieldArray({
        control,
        name: "sourceStep.sourceData.local.restorePoints",
    });

    const { resourcesService } = useServices();

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (directory, isSharded) => {
            const dto = await resourcesService.getRestorePoints_Local(
                _.trim(directory),
                null,
                true,
                isSharded ? index : undefined
            );
            return mapRestorePointDtoToSelectOptions(dto);
        },
        [directory, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="sourceStep.sourceData.local.restorePoints"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
