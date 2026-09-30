import React from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { FormInput, FormLabel } from "components/common/Form";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import AzureAuthTypeToggle from "components/common/formDestinations/AzureAuthTypeToggle";
import { mapAzureCredentialsToDto } from "components/common/formDestinations/utils/formDestinationsMapsToDto";

export default function BackupSourceAzure() {
    const { control } = useFormContext<FormData>();
    const authType = useWatch({ control, name: "sourceStep.sourceData.azure.authType" });

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">Account Name</FormLabel>
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
                <Col lg={{ offset: 3 }}>
                    <AzureAuthTypeToggle control={control} name="sourceStep.sourceData.azure.authType" />
                </Col>
            </Row>
            {authType === "sasToken" ? (
                <Row className="mt-2">
                    <Col lg="3">
                        <FormLabel className="col-form-label">SAS Token</FormLabel>
                    </Col>
                    <Col>
                        <FormInput
                            key="sasToken"
                            type="password"
                            control={control}
                            name="sourceStep.sourceData.azure.sasToken"
                            placeholder="Enter Azure Storage SAS Token"
                            passwordPreview
                        />
                    </Col>
                </Row>
            ) : (
                <Row className="mt-2">
                    <Col lg="3">
                        <FormLabel className="col-form-label">Account Key</FormLabel>
                    </Col>
                    <Col>
                        <FormInput
                            key="accountKey"
                            type="password"
                            control={control}
                            name="sourceStep.sourceData.azure.accountKey"
                            placeholder="Enter Azure Storage Account Key"
                            passwordPreview
                        />
                    </Col>
                </Row>
            )}
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">Container</FormLabel>
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
                    <FormLabel className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </FormLabel>
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
        const credentials = mapAzureCredentialsToDto(azureData);
        const hasCredentials = !!credentials.AccountKey || !!credentials.SasToken;

        if (!azureData.accountName || !hasCredentials || !azureData.container) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_AzureBackup(
            {
                ...credentials,
                AccountName: azureData.accountName,
                StorageContainer: azureData.container,
                RemoteFolderName: azureData.remoteFolderName,
                Disabled: false,
                GetBackupConfigurationScript: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [
        azureData.authType,
        azureData.accountName,
        azureData.accountKey,
        azureData.sasToken,
        azureData.container,
        azureData.remoteFolderName,
        isSharded,
    ]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
