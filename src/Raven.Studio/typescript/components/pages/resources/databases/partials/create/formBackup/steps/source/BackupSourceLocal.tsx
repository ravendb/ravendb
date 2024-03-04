import { FormSelectAutocomplete } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { useServices } from "components/hooks/useServices";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Col, Row } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { restorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/restorePointUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceLocal() {
    const { resourcesService } = useServices();
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: {
                local: { directory },
            },
        },
    } = useWatch({
        control,
    });

    const asyncGetLocalFolderPathOptions = useAsyncDebounce(
        async (directory) => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(directory, true);

            return dto.List.map((x) => ({ value: x, label: x }) satisfies SelectOption);
        },
        [directory]
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
                mapRestorePoint={(field, index) => <SourceRestorePoint key={field.id} index={index} />}
            />
            <EncryptionField sourceType="local" />
        </>
    );
}

function SourceRestorePoint({ index }: { index: number }) {
    const { resourcesService } = useServices();
    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: {
                local: { directory },
            },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (directory, isSharded) => {
            if (!directory) {
                return [];
            }

            const dto = await resourcesService.getRestorePoints_Local(
                _.trim(directory),
                null,
                true,
                isSharded ? index : undefined
            );
            return restorePointUtils.mapToSelectOptions(dto);
        },
        [directory, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
