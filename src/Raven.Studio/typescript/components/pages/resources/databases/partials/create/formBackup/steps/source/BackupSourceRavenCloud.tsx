import React from "react";
import { Row, Col, Label, UncontrolledPopover, PopoverBody } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useServices } from "components/hooks/useServices";
import { FormInput } from "components/common/Form";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import moment from "moment";
import generalUtils from "common/generalUtils";

export default function BackupSourceRavenCloud() {
    const { control } = useFormContext<FormData>();

    const {
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
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="ravenCloud" />
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

function SourceRestorePoint({ index, remove }: RestorePointElementProps) {
    const { resourcesService } = useServices();
    const { mapToSelectOptions } = useRestorePointUtils();
    const { trigger, setValue, control } = useFormContext<FormData>();

    const {
        basicInfoStep: { isSharded },
        sourceStep: {
            sourceData: { ravenCloud },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (link) => {
            if (!link || !(await trigger("sourceStep.sourceData.ravenCloud.link"))) {
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
                    AwsSessionToken: credentials.AwsSessionToken,
                    AwsAccessKey: credentials.AwsAccessKey,
                    AwsSecretKey: credentials.AwsSecretKey,
                    AwsRegionName: credentials.AwsRegionName,
                    RemoteFolderName: credentials.RemoteFolderName,
                    BucketName: credentials.BucketName,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    CustomServerUrl: null, // TODO RavenDB-14716
                    ForcePathStyle: false,
                },
                true,
                isSharded ? index : undefined
            );
            return mapToSelectOptions(dto);
        },
        [ravenCloud.link]
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
