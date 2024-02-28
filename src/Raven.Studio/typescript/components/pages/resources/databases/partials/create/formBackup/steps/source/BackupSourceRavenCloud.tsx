import React from "react";
import { Row, Col, Label, UncontrolledPopover, PopoverBody } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useServices } from "components/hooks/useServices";
import { FormInput } from "components/common/Form";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { restorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/restorePointUtils";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import moment from "moment";
import generalUtils from "common/generalUtils";

export default function BackupSourceRavenCloud() {
    const { control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { ravenCloud: ravenCloudData },
        },
    } = useWatch({
        control,
    });

    const expireDateMoment = ravenCloudData.awsSettings?.expireDate
        ? moment.utc(ravenCloudData.awsSettings.expireDate)
        : null;

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <LinkLabel />
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.ravenCloud.link"
                        placeholder="Enter backup link generated in RavenDB Cloud"
                    />
                </Col>
            </Row>
            {expireDateMoment && (
                <Row className="mt-2 align-items-center">
                    <Col lg="3">
                        <Label className="col-form-label">Link will expire in</Label>
                    </Col>
                    <Col>
                        <Icon icon="clock" />
                        {expireDateMoment.isBefore()
                            ? "Link has expired"
                            : generalUtils.formatDurationByDate(expireDateMoment)}
                    </Col>
                </Row>
            )}
            <RestorePointsFields
                isSharded={isSharded}
                pointsWithTagsFieldName="sourceStep.sourceData.ravenCloud.pointsWithTags"
                mapRestorePoint={(field, index) => (
                    <RavenCloudSourceRestorePoint
                        key={field.id}
                        index={index}
                        isSharded={isSharded}
                        link={ravenCloudData.link}
                    />
                )}
            />
            <EncryptionField
                encryptionKeyFieldName="sourceStep.sourceData.ravenCloud.encryptionKey"
                selectedSourceData={ravenCloudData}
            />
        </>
    );
}

function LinkLabel() {
    return (
        <>
            <Label className="col-form-label" id="CloudBackupLinkInfo">
                Backup Link <Icon icon="info" color="info" margin="m-0" />
            </Label>
            <UncontrolledPopover target="CloudBackupLinkInfo" trigger="hover" className="bs5">
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
        </>
    );
}

interface RavenCloudSourceRestorePointProps {
    index: number;
    isSharded: boolean;
    link: string;
}

function RavenCloudSourceRestorePoint({ index, isSharded, link }: RavenCloudSourceRestorePointProps) {
    const { resourcesService } = useServices();
    const { control, setValue } = useFormContext<FormData>();

    const { remove } = useFieldArray({
        control,
        name: "sourceStep.sourceData.local.pointsWithTags",
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (link) => {
            if (!link) {
                return [];
            }

            const credentials = await resourcesService.getCloudBackupCredentialsFromLink(link);

            setValue("sourceStep.sourceData.ravenCloud.awsSettings", {
                sessionToken: credentials.AwsSessionToken,
                accessKey: credentials.AwsAccessKey,
                secretKey: credentials.AwsSecretKey,
                regionName: credentials.AwsRegionName,
                remoteFolderName: credentials.RemoteFolderName,
                bucketName: credentials.BucketName,
                expireDate: credentials.Expires,
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
            return restorePointUtils.mapToSelectOptions(dto);
        },
        [link]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="sourceStep.sourceData.ravenCloud.pointsWithTags"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
