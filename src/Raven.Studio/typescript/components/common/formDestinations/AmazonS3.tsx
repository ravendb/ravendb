import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { Card, CardBody, Collapse, UncontrolledPopover, PopoverBody, Label } from "reactstrap";
import { FlexGrow } from "../FlexGrow";
import { FormSwitch, FormInput, FormSelect } from "../Form";
import OverrideConfiguration from "./OverrideConfiguration";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapFtpToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";

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

        return manageServerService.testPeriodicBackupCredentials("S3", mapFtpToDto(formValues));
    });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Amazon S3
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="mt-2">
                    <FormSwitch
                        control={control}
                        name={getName("isOverrideConfig")}
                        className="ms-3 mb-2 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
                    ) : (
                        <>
                            <div className="ms-3">
                                <FormSwitch
                                    control={control}
                                    name={getName("isUseCustomHost")}
                                    className="mb-2 w-100"
                                    color="secondary"
                                >
                                    Use a custom S3 host
                                </FormSwitch>
                                {formValues.isUseCustomHost && (
                                    <>
                                        <FormSwitch
                                            control={control}
                                            name={getName("forcePathStyle")}
                                            className="ms-3 mb-2 w-100"
                                            color="secondary"
                                        >
                                            Force path style{" "}
                                            <Icon icon="info" color="info" id="forcePathStyleTooltip" />
                                        </FormSwitch>
                                        <UncontrolledPopover
                                            target="forcePathStyleTooltip"
                                            trigger="hover"
                                            placement="top"
                                            className="bs5"
                                        >
                                            <PopoverBody>
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
                                            </PopoverBody>
                                        </UncontrolledPopover>
                                    </>
                                )}
                            </div>
                            {formValues.isUseCustomHost && (
                                <div>
                                    <Label className="mb-0 md-label">Custom server URL</Label>
                                    <FormInput
                                        control={control}
                                        name={getName("customServerUrl")}
                                        placeholder="Enter a custom server URL"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                            )}
                            <div>
                                <Label className="mb-0 md-label">
                                    Bucket name
                                    <Icon icon="info" color="info" id="bucketNameTooltip" />
                                </Label>
                                <UncontrolledPopover
                                    target="bucketNameTooltip"
                                    trigger="hover"
                                    placement="top"
                                    className="bs5"
                                >
                                    <PopoverBody>
                                        Bucket should be created manually in order for this OLAP to work. You can use
                                        the <span className="text-info">Test credentials</span> button to verify its
                                        existence.
                                    </PopoverBody>
                                </UncontrolledPopover>
                                <FormInput
                                    control={control}
                                    name={getName("bucketName")}
                                    placeholder="Enter a bucket name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">
                                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                                </Label>
                                <FormInput
                                    control={control}
                                    name={getName("remoteFolderName")}
                                    placeholder="Enter a remote folder name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">
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
                                        className="mb-2"
                                    />
                                ) : (
                                    <FormSelect
                                        name={getName("awsRegionName")}
                                        control={control}
                                        placeholder="Select an AWS region"
                                        options={allRegionsOptions}
                                        className="mb-2"
                                    />
                                )}
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Access key</Label>
                                <FormInput
                                    name={getName("awsAccessKey")}
                                    control={control}
                                    placeholder="Enter an access key"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Secret key</Label>
                                <FormInput
                                    name={getName("awsSecretKey")}
                                    control={control}
                                    placeholder="Enter a secret key"
                                    type="text"
                                />
                            </div>
                            <div className="d-flex mt-3">
                                <FlexGrow />
                                <ButtonWithSpinner
                                    type="button"
                                    color="info"
                                    onClick={asyncTest.execute}
                                    isSpinning={asyncTest.loading}
                                >
                                    <Icon icon="rocket" />
                                    Test credentials
                                </ButtonWithSpinner>
                            </div>
                            <div className="mt-2">
                                <ConnectionTestResult testResult={asyncTest.result} />
                            </div>
                        </>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

const allRegions = exhaustiveStringTuple()("Africa", "Europe", "America", "Asia");

const allRegionsOptions: SelectOption[] = allRegions.map((type) => ({
    value: type,
    label: type,
}));

const fieldBase = "destinations.s3";

type FormFieldNames = keyof FormDestinations["destinations"]["s3"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
