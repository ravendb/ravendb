import React from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { FormInput } from "components/common/Form";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceAzure() {
    const { control } = useFormContext<FormData>();

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <Form.Label className="col-form-label">Account Name</Form.Label>
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
                    <Form.Label className="col-form-label">Account Key</Form.Label>
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
                    <Form.Label className="col-form-label">Container</Form.Label>
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
                    <Form.Label className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </Form.Label>
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
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="azure" />
        </div>
    );
}

function SourceRestorePoint({ index, remove }: RestorePointElementProps) {
    const { resourcesService } = useServices();
    const { mapToSelectOptions } = useRestorePointUtils();

    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { azure: azureData },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!azureData.accountName || !azureData.accountKey || !azureData.container) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_AzureBackup(
            {
                AccountKey: azureData.accountKey,
                AccountName: azureData.accountName,
                StorageContainer: azureData.container,
                RemoteFolderName: azureData.remoteFolderName,
                Disabled: false,
                GetBackupConfigurationScript: null,
                SasToken: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [azureData.accountName, azureData.accountKey, azureData.container, azureData.remoteFolderName, isSharded]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
