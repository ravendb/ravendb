import React from "react";
import { useFormContext, useWatch, useFieldArray } from "react-hook-form";
import { Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FormInput } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { mapRestorePointDtoToSelectOptions } from "components/pages/resources/databases/partials/create/formBackup/steps/source/backupSourceUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceGoogleCloud() {
    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { googleCloud: googleCloudData },
        },
    } = useWatch({
        control,
    });

    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.bucketName"
                        placeholder="Enter Google Cloud Storage Bucket Name"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Google Credentials</Label>
                </Col>
                <Col>
                    <FormInput
                        type="textarea"
                        rows="18"
                        control={control}
                        name="sourceStep.sourceData.googleCloud.credentialsJson"
                        placeholder={credentialsPlaceholder}
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
                        name="sourceStep.sourceData.googleCloud.remoteFolderName"
                        placeholder="Enter Remote Folder Name"
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
                        googleCloudData={googleCloudData}
                        isSharded={isSharded}
                    />
                )}
            />
            <EncryptionField
                encryptionKeyFieldName="sourceStep.sourceData.googleCloud.encryptionKey"
                selectedSourceData={googleCloudData}
            />
        </div>
    );
}

interface SourceRestorePointProps {
    index: number;
    googleCloudData: FormData["sourceStep"]["sourceData"]["googleCloud"];
    isSharded: boolean;
}

function SourceRestorePoint({ index, googleCloudData, isSharded }: SourceRestorePointProps) {
    const { resourcesService } = useServices();

    const { control } = useFormContext<FormData>();

    const { remove } = useFieldArray({
        control,
        name: "sourceStep.sourceData.googleCloud.restorePoints",
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (bucketName, credentialsJson, remoteFolderName) => {
            const dto = await resourcesService.getRestorePoints_GoogleCloudBackup(
                {
                    BucketName: bucketName,
                    GoogleCredentialsJson: credentialsJson,
                    RemoteFolderName: remoteFolderName,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                },
                true,
                isSharded ? index : undefined
            );
            return mapRestorePointDtoToSelectOptions(dto);
        },
        [googleCloudData.bucketName, googleCloudData.credentialsJson, googleCloudData.remoteFolderName]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="sourceStep.sourceData.googleCloud.restorePoints"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}

const credentialsPlaceholder = `e.g.
{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=\n-----END PRIVATE KEY-----\n",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}`;
