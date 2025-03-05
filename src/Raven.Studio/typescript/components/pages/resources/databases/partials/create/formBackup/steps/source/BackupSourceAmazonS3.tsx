import { Icon } from "components/common/Icon";
import Collapse from "react-bootstrap/Collapse";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {  Label } from "reactstrap";
import { useFormContext, useWatch } from "react-hook-form";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FormInput, FormSelectAutocomplete, FormSwitch } from "components/common/Form";
import { useServices } from "components/hooks/useServices";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { useRestorePointUtils } from "components/pages/resources/databases/partials/create/formBackup/steps/source/useRestorePointUtils";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { availableS3Regions } from "components/utils/common";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields, {
    RestorePointElementProps,
} from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export default function BackupSourceAmazonS3() {
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: {
                amazonS3: { isUseCustomHost },
            },
        },
    } = useWatch({
        control,
    });

    return (
        <div className="mt-2">
            <Row>
                <Col lg={{ offset: 3 }}>
                    <FormSwitch control={control} name="sourceStep.sourceData.amazonS3.isUseCustomHost" color="primary">
                        Use a custom S3 host
                    </FormSwitch>
                </Col>
            </Row>

            <Collapse in={isUseCustomHost}>
                <div>
                    <Row>
                        <Col lg={{ offset: 3 }}>
                            <FormSwitch
                                color="primary"
                                control={control}
                                name="sourceStep.sourceData.amazonS3.isForcePathStyle"
                            >
                                Force path style{" "}
                                <PopoverWithHoverWrapper
                                    message={
                                        <>
                                            Whether to force path style URLs for S3 objects (e.g.,{" "}
                                            <code>https://&#123;Server-URL&#125;/&#123;Bucket-Name&#125;</code> instead
                                            of <code>https://&#123;Bucket-Name&#125;.&#123;Server-URL&#125;</code>)
                                        </>
                                    }
                                >
                                    <Icon icon="info" color="info" margin="m-0" />
                                </PopoverWithHoverWrapper>
                            </FormSwitch>
                        </Col>
                    </Row>
                    <Row className="mt-2">
                        <Col lg="3">
                            <Label className="col-form-label">Custom server URL</Label>
                        </Col>
                        <Col>
                            <FormInput
                                type="text"
                                control={control}
                                name="sourceStep.sourceData.amazonS3.customHost"
                                placeholder="Enter custom server URL"
                            />
                        </Col>
                    </Row>
                </div>
            </Collapse>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label" check>
                        Access key
                    </Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.accessKey"
                        placeholder="Enter access key"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label" aria-required>
                        Secret key
                    </Label>
                </Col>
                <Col>
                    <FormInput
                        control={control}
                        name="sourceStep.sourceData.amazonS3.secretKey"
                        placeholder="Enter secret key"
                        type="password"
                        passwordPreview
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">
                        Aws Region
                        <br />
                        {isUseCustomHost && <small>(optional)</small>}
                    </Label>
                </Col>
                <Col>
                    {isUseCustomHost ? (
                        <FormInput
                            type="text"
                            control={control}
                            name="sourceStep.sourceData.amazonS3.awsRegion"
                            placeholder="Enter an AWS region"
                            autoComplete="off"
                        />
                    ) : (
                        <FormSelectAutocomplete
                            name="sourceStep.sourceData.amazonS3.awsRegion"
                            control={control}
                            placeholder="Select an AWS region (or enter new one)"
                            options={availableS3Regions}
                        />
                    )}
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.bucketName"
                        placeholder="Enter bucket name"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">
                        Remote Folder Name
                        <br />
                        <small>(optional)</small>
                    </Label>
                </Col>
                <Col>
                    <FormInput
                        type="text"
                        control={control}
                        name="sourceStep.sourceData.amazonS3.remoteFolderName"
                        placeholder="Enter remote folder name"
                    />
                </Col>
            </Row>
            <RestorePointsFields restorePointElement={SourceRestorePoint} />
            <EncryptionField sourceType="amazonS3" />
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
            sourceData: { amazonS3: amazonS3Data },
        },
    } = useWatch({
        control,
    });

    const asyncGetRestorePointsOptions = useAsyncDebounce(async () => {
        if (!amazonS3Data.accessKey || !amazonS3Data.secretKey || !amazonS3Data.awsRegion || !amazonS3Data.bucketName) {
            return [];
        }

        const dto = await resourcesService.getRestorePoints_S3Backup(
            {
                AwsAccessKey: amazonS3Data.accessKey,
                AwsSecretKey: amazonS3Data.secretKey,
                AwsRegionName: amazonS3Data.awsRegion,
                BucketName: amazonS3Data.bucketName,
                RemoteFolderName: amazonS3Data.remoteFolderName,
                AwsSessionToken: "",
                CustomServerUrl: amazonS3Data.isUseCustomHost ? amazonS3Data.customHost : null,
                ForcePathStyle: amazonS3Data.isUseCustomHost && amazonS3Data.isForcePathStyle,
                Disabled: false,
                GetBackupConfigurationScript: null,
            },
            true,
            isSharded ? index : undefined
        );
        return mapToSelectOptions(dto);
    }, [
        amazonS3Data.accessKey,
        amazonS3Data.secretKey,
        amazonS3Data.awsRegion,
        amazonS3Data.bucketName,
        amazonS3Data.remoteFolderName,
        amazonS3Data.isUseCustomHost,
        amazonS3Data.customHost,
        amazonS3Data.isForcePathStyle,
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
