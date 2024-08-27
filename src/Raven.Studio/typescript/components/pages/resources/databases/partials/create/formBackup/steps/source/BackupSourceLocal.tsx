import { FormPathSelector } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Col, Row } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceLocal() {
    const { resourcesService } = useServices();
    const { control } = useFormContext<FormData>();

    const getLocalFolderPaths = async (path: string) => {
        const dto = await resourcesService.getFolderPathOptions_ServerLocal(path, true);
        return dto?.List || [];
    };

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <label className="col-form-label">Directory Path</label>
                </Col>
                <Col>
                    <FormPathSelector
                        control={control}
                        name="sourceStep.sourceData.local.directory"
                        selectorTitle="Select backup directory path"
                        placeholder="Enter backup directory path"
                        getPaths={getLocalFolderPaths}
                        getPathDependencies={(path: string) => [path]}
                    />
                </Col>
            </Row>
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="local" />
        </>
    );
}

function SourceRestorePoint({ index, remove }: RestorePointElementProps) {
    const { resourcesService } = useServices();
    const { control } = useFormContext<FormData>();
    const { mapToSelectOptions } = useRestorePointUtils();

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
            return mapToSelectOptions(dto);
        },
        [directory, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
