import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import Badge from "react-bootstrap/Badge";
import { Card, CardBody, Collapse, Label } from "reactstrap";
import { FormSwitch, FormInput, FormSelectCreatable } from "../Form";
import OverrideConfiguration from "./OverrideConfiguration";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapS3ToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import { FlexGrow } from "components/common/FlexGrow";
import { availableS3Regions } from "components/utils/common";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";

export default function AmazonS3() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { s3: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("S3", mapS3ToDto(formValues));
    });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Amazon S3
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <FormSwitch
                        control={control}
                        name={`${fieldBase}.config.isOverrideConfig`}
                        className="ms-3 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.config.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
                    ) : (
                        <>
                            <div className="vstack gap-2">
                                <FormSwitch
                                    control={control}
                                    name={getName("isUseCustomHost")}
                                    className="w-100"
                                    color="secondary"
                                >
                                    Use a custom S3 host
                                </FormSwitch>
                                {formValues.isUseCustomHost && (
                                    <FormSwitch
                                        control={control}
                                        name={getName("forcePathStyle")}
                                        className="w-100"
                                        color="secondary"
                                    >
                                        <span className="d-flex gap-1 align-items-center">
                                            Force path style
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <>
                                                        Whether to force path style URLs for S3 objects (e.g.,{" "}
                                                        <code>
                                                            https://{`{Server-URL}`}/{`{Bucket-Name}`}
                                                        </code>{" "}
                                                        instead of{" "}
                                                        <code>
                                                            {`{https://`}
                                                            {`{Bucket-Name}`}.{`{Server-URL}`}
                                                            {`}`}
                                                        </code>
                                                        )
                                                    </>
                                                }
                                            >
                                                <Icon icon="info" color="info" />
                                            </PopoverWithHoverWrapper>
                                        </span>
                                    </FormSwitch>
                                )}
                            </div>
                            <div className="vstack gap-3 mt-2">
                                {formValues.isUseCustomHost && (
                                    <div className="mb-2">
                                        <Label>Custom server URL</Label>
                                        <FormInput
                                            control={control}
                                            name={getName("customServerUrl")}
                                            placeholder="Enter a custom server URL"
                                            type="text"
                                            autoComplete="off"
                                        />
                                    </div>
                                )}
                                <div className="mb-2">
                                    <Label className="d-flex align-items-center gap-1">
                                        Bucket name
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    Bucket should be created manually in order for this OLAP to work.
                                                    You can use the <span className="text-info">Test credentials</span>{" "}
                                                    button to verify its existence.
                                                </>
                                            }
                                        >
                                            <Icon icon="info" color="info" margin="m-0" />
                                        </PopoverWithHoverWrapper>
                                        {asyncTest.result?.Success ? (
                                            <Badge bg="success" pill>
                                                <Icon icon="check" />
                                                Successfully connected
                                            </Badge>
                                        ) : asyncTest.result?.Error ? (
                                            <Badge bg="danger" pill>
                                                <Icon icon="warning" />
                                                Failed connection
                                            </Badge>
                                        ) : null}
                                    </Label>
                                    <FormInput
                                        control={control}
                                        name={getName("bucketName")}
                                        placeholder="Enter a bucket name"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>
                                        Remote folder name <small className="text-muted fw-light">(optional)</small>
                                    </Label>
                                    <FormInput
                                        control={control}
                                        name={getName("remoteFolderName")}
                                        placeholder="Enter a remote folder name"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>
                                        Region{" "}
                                        {formValues.isUseCustomHost && (
                                            <small className="text-muted fw-light">(optional)</small>
                                        )}
                                    </Label>
                                    {formValues.isUseCustomHost ? (
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name={getName("awsRegionName")}
                                            placeholder="Enter an AWS region"
                                            autoComplete="off"
                                        />
                                    ) : (
                                        <FormSelectCreatable
                                            name={getName("awsRegionName")}
                                            control={control}
                                            placeholder="Select an AWS region (or enter new one)"
                                            options={availableS3Regions}
                                        />
                                    )}
                                </div>
                                <div className="mb-2">
                                    <Label>Access key</Label>
                                    <FormInput
                                        name={getName("awsAccessKey")}
                                        control={control}
                                        placeholder="Enter an access key"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>Secret key</Label>
                                    <FormInput
                                        name={getName("awsSecretKey")}
                                        control={control}
                                        placeholder="Enter a secret key"
                                        type="password"
                                        passwordPreview
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="d-flex justify-content-end">
                                    <FlexGrow />
                                    <ButtonWithSpinner
                                        type="button"
                                        variant="secondary"
                                        onClick={asyncTest.execute}
                                        isSpinning={asyncTest.loading}
                                        icon="rocket"
                                    >
                                        Test credentials
                                    </ButtonWithSpinner>
                                </div>
                            </div>
                            {asyncTest.result?.Error && (
                                <div className="mt-3">
                                    <ConnectionTestResult testResult={asyncTest.result} />
                                </div>
                            )}
                        </>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

const fieldBase = "destinations.s3";

type FormFieldNames = keyof FormDestinations["destinations"]["s3"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
