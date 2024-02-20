import React from "react";
import { Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch, useFieldArray } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { mapRestorePointDtoToSelectOptions } from "components/pages/resources/databases/partials/create/formBackup/steps/source/backupSourceUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { FormInput } from "components/common/Form";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceAzure() {
    const { control } = useFormContext<FormData>();

    const {
        basicInfo: { isSharded },
        source: {
            sourceData: { azure: azureData },
        },
    } = useWatch({
        control,
    });

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
                        name="source.sourceData.azure.accountName"
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
                        name="source.sourceData.azure.accountKey"
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
                        name="source.sourceData.azure.container"
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
                        name="source.sourceData.azure.remoteFolderName"
                        placeholder="Enter remote folder name"
                    />
                </Col>
            </Row>
            <RestorePointsFields
                isSharded={isSharded}
                restorePointsFieldName="source.sourceData.azure.restorePoints"
                mapRestorePoint={(field, index) => (
                    <SourceRestorePoint key={field.id} index={index} azureData={azureData} isSharded={isSharded} />
                )}
            />
            <EncryptionField
                encryptionKeyFieldName="source.sourceData.azure.encryptionKey"
                selectedSourceData={azureData}
            />
        </div>
    );
}

interface SourceRestorePointProps {
    index: number;
    azureData: FormData["source"]["sourceData"]["azure"];
    isSharded: boolean;
}

function SourceRestorePoint({ index, azureData, isSharded }: SourceRestorePointProps) {
    const { resourcesService } = useServices();

    const { control } = useFormContext<FormData>();
    const { remove } = useFieldArray({
        control,
        name: "source.sourceData.azure.restorePoints",
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (accountName, accountKey, container, remoteFolderName, isSharded) => {
            const dto = await resourcesService.getRestorePoints_AzureBackup(
                {
                    AccountKey: _.trim(accountKey),
                    AccountName: _.trim(accountName),
                    StorageContainer: _.trim(container),
                    RemoteFolderName: _.trim(remoteFolderName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    SasToken: null,
                },
                true,
                isSharded ? index : undefined
            );
            return mapRestorePointDtoToSelectOptions(dto);
        },
        [azureData.accountName, azureData.accountKey, azureData.container, azureData.remoteFolderName, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="source.sourceData.azure.restorePoints"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
