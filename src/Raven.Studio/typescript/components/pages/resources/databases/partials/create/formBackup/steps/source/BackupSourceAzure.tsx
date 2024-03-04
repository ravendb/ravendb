import React from "react";
import { Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { restorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/restorePointUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { FormInput } from "components/common/Form";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceAzure() {
    const { control } = useFormContext<FormData>();

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Account Name</Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.azure.accountName"
                        placeholder="Enter Azure Storage Account Name"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Account Key</Label>
                </Col>
                <Col>
                    <FormInput
                        type="password"
                        control={control}
                        name="sourceStep.sourceData.azure.accountKey"
                        placeholder="Enter Azure Storage Account Key"
                        passwordPreview
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Container</Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.azure.container"
                        placeholder="Enter Azure Storage Container Name"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.azure.remoteFolderName"
                        placeholder="Enter remote folder name"
                    />
                </Col>
            </Row>
            <RestorePointsFields
                mapRestorePoint={(field, index) => <SourceRestorePoint key={field.id} index={index} />}
            />
            <EncryptionField sourceType="azure" />
        </div>
    );
}

function SourceRestorePoint({ index }: { index: number }) {
    const { resourcesService } = useServices();

    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { azure: azureData },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (accountName, accountKey, container, remoteFolderName, isSharded) => {
            if (!accountName || !accountKey || !container) {
                return [];
            }

            const dto = await resourcesService.getRestorePoints_AzureBackup(
                {
                    AccountKey: accountKey,
                    AccountName: accountName,
                    StorageContainer: container,
                    RemoteFolderName: remoteFolderName,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    SasToken: null,
                },
                true,
                isSharded ? index : undefined
            );
            return restorePointUtils.mapToSelectOptions(dto);
        },
        [azureData.accountName, azureData.accountKey, azureData.container, azureData.remoteFolderName, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
