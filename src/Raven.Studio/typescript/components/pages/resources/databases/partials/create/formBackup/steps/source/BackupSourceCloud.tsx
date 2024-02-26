import React from "react";
import { Row, Col, Label, UncontrolledPopover, PopoverBody } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useServices } from "components/hooks/useServices";
import { FormInput } from "components/common/Form";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { mapRestorePointDtoToSelectOptions } from "components/pages/resources/databases/partials/create/formBackup/steps/source/backupSourceUtils";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import moment from "moment";
import generalUtils from "common/generalUtils";

export default function BackupSourceCloud() {
    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { cloud: cloudData },
        },
    } = useWatch({
        control,
    });

    const expireDateMoment = cloudData.awsSettings?.expireDate ? moment.utc(cloudData.awsSettings.expireDate) : null;

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label" id="CloudBackupLinkInfo">
                        Backup Link <Icon icon="info" color="info" margin="m-0" />
                    </Label>
                    <UncontrolledPopover target="CloudBackupLinkInfo" trigger="hover">
                        <PopoverBody>
                            <ol className="m-0">
                                <li>
                                    Login to your <code>RavenDB Cloud Account</code>
                                </li>
                                <li>
                                    Go to <code>Backups</code> view
                                </li>
                                <li>Select desired Instance and a Backup File</li>
                                <li>
                                    Click <code>Generate Backup Link</code>
                                </li>
                            </ol>
                        </PopoverBody>
                    </UncontrolledPopover>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.cloud.link"
                        placeholder="Enter backup link generated in RavenDB Cloud"
                    />
                </Col>
            </Row>
            {expireDateMoment && (
                <Row className="mt-2">
                    <Col lg="3">
                        <Label className="col-form-label">Link will expire in</Label>
                    </Col>
                    <Col>
                        {expireDateMoment.isBefore()
                            ? "Link has expired"
                            : generalUtils.formatDurationByDate(expireDateMoment)}
                    </Col>
                </Row>
            )}
            <RestorePointsFields
                isSharded={isSharded}
                restorePointsFieldName="sourceStep.sourceData.cloud.restorePoints"
                mapRestorePoint={(field, index) => (
                    <CloudSourceRestorePoint key={field.id} index={index} isSharded={isSharded} link={cloudData.link} />
                )}
            />
            <EncryptionField
                encryptionKeyFieldName="sourceStep.sourceData.cloud.encryptionKey"
                selectedSourceData={cloudData}
            />
        </>
    );
}

interface CloudSourceRestorePointProps {
    index: number;
    isSharded: boolean;
    link: string;
}

function CloudSourceRestorePoint({ index, isSharded, link }: CloudSourceRestorePointProps) {
    const { resourcesService } = useServices();
    const { control, setValue } = useFormContext<FormData>();

    const { remove } = useFieldArray({
        control,
        name: "sourceStep.sourceData.local.restorePoints",
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (link) => {
            const credentials = await resourcesService.getCloudBackupCredentialsFromLink(link);

            setValue("sourceStep.sourceData.cloud.awsSettings", {
                sessionToken: credentials.AwsSessionToken,
                accessKey: credentials.AwsAccessKey,
                secretKey: credentials.AwsSecretKey,
                regionName: credentials.AwsRegionName,
                remoteFolderName: credentials.RemoteFolderName,
                bucketName: credentials.BucketName,
                expireDate: credentials.Expires,
                disabled: false,
                getBackupConfigurationScript: null,
                customServerUrl: null, // TODO RavenDB-14716
                forcePathStyle: false,
            });

            const dto = await resourcesService.getRestorePoints_S3Backup(
                {
                    AwsSessionToken: _.trim(credentials.AwsSessionToken),
                    AwsAccessKey: _.trim(credentials.AwsAccessKey),
                    AwsSecretKey: _.trim(credentials.AwsSecretKey),
                    AwsRegionName: _.trim(credentials.AwsRegionName),
                    RemoteFolderName: _.trim(credentials.RemoteFolderName),
                    BucketName: _.trim(credentials.BucketName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    CustomServerUrl: null, // TODO RavenDB-14716
                    ForcePathStyle: false,
                },
                true,
                isSharded ? index : undefined
            );
            return mapRestorePointDtoToSelectOptions(dto);
        },
        [link]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="sourceStep.sourceData.cloud.restorePoints"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
