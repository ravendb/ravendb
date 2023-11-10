import React, { useState } from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import OverrideConfiguration from "components/common/destinations/OverrideConfiguration";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import { FlexGrow } from "components/common/FlexGrow";

interface AmazonGlacierProps {
    className?: string;
}
const AmazonGlacier = (props: AmazonGlacierProps) => {
    const { className } = props;
    const { control } = useForm<null>({});
    const [isOpen, setIsOpen] = useState(false);
    const [isOverrideConfigurationEnabled, setOverrideConfiguration] = useState(false);
    const toggle = () => setIsOpen(!isOpen);
    const toggleOverrideConfiguration = () => setOverrideConfiguration(!isOverrideConfigurationEnabled);

    const allRegions = exhaustiveStringTuple()("Africa", "Europe", "America", "Asia");

    const allRegionsOptions: SelectOption[] = allRegions.map((type) => ({
        value: type,
        label: type,
    }));

    return (
        <div className={className}>
            <Card className="well">
                <CardBody>
                    <FormSwitch name="amazonGlacier" control={control} onChange={toggle}>
                        Amazon Glacier
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
                                <div>
                                    <Label className="mb-0 md-label">
                                        Vault name
                                        <Icon icon="info" color="info" id="vaultNameTooltip" />
                                    </Label>
                                    <UncontrolledPopover
                                        target="vaultNameTooltip"
                                        trigger="hover"
                                        placement="top"
                                        className="bs5"
                                    >
                                        <PopoverBody>
                                            Vault should be created manually in order for this OLAP to work. You can use
                                            the <span className="text-info">Test credentials</span> button to verify its
                                            existence.
                                        </PopoverBody>
                                    </UncontrolledPopover>
                                    <FormInput
                                        name="vaultName"
                                        control={control}
                                        placeholder="Enter a vault name"
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
                                    <Label className="mb-0 md-label">Region</Label>
                                    <FormSelect
                                        name="region"
                                        control={control}
                                        placeholder="Select an AWS region"
                                        options={allRegionsOptions}
                                        className="mb-2"
                                    />
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

export default AmazonGlacier;
