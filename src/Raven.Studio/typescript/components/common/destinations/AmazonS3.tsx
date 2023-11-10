import React, { useState } from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import OverrideConfiguration from "components/common/destinations/OverrideConfiguration";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import { FlexGrow } from "components/common/FlexGrow";

interface AmazonS3Props {
    className?: string;
}
const AmazonS3 = (props: AmazonS3Props) => {
    const { className } = props;
    const { control } = useForm<null>({});
    const [isOpen, setIsOpen] = useState(false);
    const [isOverrideConfigurationEnabled, setOverrideConfiguration] = useState(false);
    const [isCustomHostEnabled, setCustomHost] = useState(false);
    const [isForcePathStyleEnabled, setForcePathStyle] = useState(false);
    const toggle = () => setIsOpen(!isOpen);
    const toggleOverrideConfiguration = () => setOverrideConfiguration(!isOverrideConfigurationEnabled);
    const toggleCustomHost = () => setCustomHost(!isCustomHostEnabled);
    const toggleForcePathStyle = () => setForcePathStyle(!isForcePathStyleEnabled);

    const allRegions = exhaustiveStringTuple()("Africa", "Europe", "America", "Asia");

    const allRegionsOptions: SelectOption[] = allRegions.map((type) => ({
        value: type,
        label: type,
    }));

    return (
        <div className={className}>
            <Card className="well">
                <CardBody>
                    <FormSwitch name="amazonS3" control={control} onChange={toggle}>
                        Amazon S3
                    </FormSwitch>
                    <Collapse isOpen={isOpen} className="mt-2">
                        <FormSwitch
                            name="overrideConfiguration"
                            control={control}
                            className="ms-3 mb-2 w-100"
                            onChange={toggleOverrideConfiguration}
                            color="secondary"
                        >
                            Override configuration via external script
                        </FormSwitch>
                        {!isOverrideConfigurationEnabled && (
                            <>
                                <div className="ms-3">
                                    <FormSwitch
                                        name="customHost"
                                        control={control}
                                        className="mb-2 w-100"
                                        onChange={toggleCustomHost}
                                        color="secondary"
                                    >
                                        Use a custom S3 host
                                    </FormSwitch>
                                    {isCustomHostEnabled && (
                                        <>
                                            <FormSwitch
                                                name="forcePathStyle"
                                                control={control}
                                                className="ms-3 mb-2 w-100"
                                                onChange={toggleForcePathStyle}
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
                                {isCustomHostEnabled && (
                                    <div>
                                        <Label className="mb-0 md-label">Custom server URL</Label>
                                        <FormInput
                                            name="customServerUrl"
                                            control={control}
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
                                            Bucket should be created manually in order for this OLAP to work. You can
                                            use the <span className="text-info">Test credentials</span> button to verify
                                            its existence.
                                        </PopoverBody>
                                    </UncontrolledPopover>
                                    <FormInput
                                        name="bucketName"
                                        control={control}
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
                                        name="remoteFolderName"
                                        control={control}
                                        placeholder="Enter a remote folder name"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">
                                        Region{" "}
                                        {isCustomHostEnabled && <small className="text-muted fw-light">(optional)</small>}
                                    </Label>
                                    {isCustomHostEnabled ? (
                                        <FormInput
                                            name="region"
                                            control={control}
                                            placeholder="Enter an AWS region"
                                            className="mb-2"
                                        />
                                    ) : (
                                        <FormSelect
                                            name="region"
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
                                        name="accessKey"
                                        control={control}
                                        placeholder="Enter an access key"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">Secret key</Label>
                                    <FormInput
                                        name="secretKey"
                                        control={control}
                                        placeholder="Enter a secret key"
                                        type="text"
                                    />
                                </div>
                                <div className="d-flex mt-3">
                                    <FlexGrow />
                                    <Button color="info">
                                        <Icon icon="rocket" />
                                        Test credentials
                                    </Button>
                                </div>
                            </>
                        )}
                        {isOverrideConfigurationEnabled && <OverrideConfiguration />}
                    </Collapse>
                </CardBody>
            </Card>
        </div>
    );
};

export default AmazonS3;
