import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FormInput, FormLabel } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceGoogleCloud() {
    const { control } = useFormContext<FormData>();

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">Bucket name</FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.bucketName"
                        placeholder="e.g. company-backups"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">Google credentials JSON</FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="textarea"
                        as="textarea"
                        rows={18}
                        control={control}
                        name="sourceStep.sourceData.googleCloud.credentialsJson"
                        placeholder="Paste the contents of your Google Cloud service account key file"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <FormLabel className="col-form-label">
                        Remote folder name <small>(optional)</small>
                    </FormLabel>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.remoteFolderName"
                        placeholder="e.g. backups/orders"
                    />
                </Col>
            </Row>
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="googleCloud" />
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
            sourceData: { googleCloud: googleCloudData },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!googleCloudData.bucketName || !googleCloudData.credentialsJson) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_GoogleCloudBackup(
            {
                BucketName: googleCloudData.bucketName,
                GoogleCredentialsJson: googleCloudData.credentialsJson,
                RemoteFolderName: googleCloudData.remoteFolderName,
                Disabled: false,
                GetBackupConfigurationScript: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [googleCloudData.bucketName, googleCloudData.credentialsJson, googleCloudData.remoteFolderName]);

    return (
        <CreateDatabaseFromBackupRestorePoint
            index={index}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
            remove={remove}
        />
    );
}
